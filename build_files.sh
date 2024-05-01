# build_files.sh
python3.9 -m venv myenv
source myenv/bin/activate
pip3 install -r requirements.txt

# make migrations
python3.9 manage.py migrate 
python3.9 manage.py collectstatic 
python3.9 manage.py runserver