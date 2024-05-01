from metaflow import FlowSpec, step 

class DataPreparation(FlowSpec):
    @step 
    def start(self):
        print("We are starting here")
        self.next(self.data_preprocess)
    
    @step
    def data_preprocess(self):
        import pandas as pd
        try:
            self.timeSeries = pd.read_csv('card_transaction.csv').query("account_to != 'IITBH'").loc[:,['record_date','account_from']]
        except Exception as e:
            print(e)
            self.next(self.end)
        
        self.mess = ['mess-galav','mess-ssai','mess-kumard']
        self.timeSeries = self.timeSeries.rename(columns={"account_from":"mess"})
        # self.timeSeries = self.timeSeries[self.timeSeries['mess']!='IITBH']
        self.timeSeries['record_date'] = pd.to_datetime(self.timeSeries['record_date'], format='%d-%m-%Y %H:%M')
        print(self.timeSeries.shape)
        self.next(self.savedifferentmessdataset,foreach='mess')
    

    @step
    def savedifferentmessdataset(self):
        self.mess_name = self.input
        self.mess_df = self.timeSeries[self.timeSeries['mess']==self.mess_name]
        print(self.input,"Here we start with shape",self.mess_df.shape)
        self.mess_df.rename(columns={"mess":"footprint"},inplace=True)
        self.mess_df= self.mess_df.set_index('record_date')
        self.mess_df = self.mess_df.resample('10T').count().reset_index().rename(columns={"record_date":"ds","footprint":"y"})
        print(f"{self.mess_name}",self.mess_df.shape)
        self.mess_df.to_csv(f'{self.mess_name}.csv')
        self.next(self.joinstep)

    @step 
    def joinstep(self,inputs):
        for x in inputs:
            print(x.input)
        self.next(self.end)

    @step
    def end(self):
        print("We are ending here")

if __name__=="__main__":
    DataPreparation()