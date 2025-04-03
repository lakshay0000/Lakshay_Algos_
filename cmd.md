# check logs on ssh 
du -sh

# Creating a Virtual Environment
python3 -m venv myenv
source myenv/bin/activate
deactivate

# To manage MongoDB through SSH
sudo systemctl status mongod
sudo systemctl restart mongod

# pm2 
pm2 ls
pm2 stop 22
pm2 restart 22
pm2 log 22
pm2 stop 22

# Apna Dabba
algo mock 1