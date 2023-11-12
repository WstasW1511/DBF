from configparser import ConfigParser


config = ConfigParser()
config.read('./config.ini')
config = config._sections
PATH = config['system']['path']
DB = config['system']['db']
prefix = config['system']['prefix']
ftp_host = config['ftp']['host']
ftp_port =config['ftp']['port']
ftp_user = config['ftp']['user']
ftp_password = config['ftp']['password']


to_write = []
all_rows = 0
accept = 0
count_error = 0
start = None
mass = []
encodding_mass =[]
