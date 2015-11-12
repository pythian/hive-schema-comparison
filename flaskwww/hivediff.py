
from flask import Flask
from flask import render_template
app = Flask(__name__, static_folder='static',static_url_path='')
import sys
import json
import schema_compare as sc
from flask.ext.wtf import Form
from wtforms import SelectField, SubmitField
import collections
from flask import request


class configuration():

  def databases(self):
    return self.config_dict['databases_to_compare']

  def environments(self):
    env_dict = {}
    for hive_host in self.config_dict['Hive_Hosts']:
       env = self.Environment(*hive_host)
       env_dict[env.host_friendlyname]={}
       env_dict[env.host_friendlyname]['host_addr']=env.host_addr
       env_dict[env.host_friendlyname]['host_port']=env.host_port
    return env_dict

  def __init__(self,config_file_path):
    self.Environment = collections.namedtuple('Environment', ['host_friendlyname', 'host_addr','host_port'])
    self.config_dict = json.load(open(config_file_path))




# pass in arguments for schema files
# and config path
try:
    config_file_path = sys.argv[1]
    schema_files_path = sys.argv[2]
except:
    print 'usage: hivediff.py <config_file_path> <schema_files_path>'
    sys.exit(2)


cfg = configuration(config_file_path)
environments = cfg.environments()

for e in cfg.environments():
    print "{0}{1}_schema.out".format(schema_files_path,e)




@app.route('/page2')
def p2():

	return render_template("page2.html")


@app.route('/db_compare/<database>/<env_a>/<env_b>')
def db_compare(database="", env_a="", env_b=""):
    print "DB COMPARE"
    dc = sc.DictCompare(schema_files_path,env_a, env_b)
    comparison_rez =  dc.compare_dbs(database)
    return render_template("compare_inner.html", database=database, res = comparison_rez, a_friendlyname = env_a, b_friendlyname=env_b)



class EnvForm(Form):

    ddl_choices = [(e,e) for e in environments]

    default_choice_a = ""
    default_choice_b = ""
    if len(ddl_choices)>1:
        default_choice_a = ddl_choices[0][0]
        default_choice_b = ddl_choices[1][0]
    env_a = SelectField('A',choices=ddl_choices, default=default_choice_a)
    env_b =  SelectField('A',choices=ddl_choices, default=default_choice_b)

    submit = SubmitField('GO')



@app.route('/home.html')
@app.route('/home')
def homepage():
    form=EnvForm(csrf_enabled=False)
    env_a = form.env_a.data
    env_b = form.env_b.data
    dc = sc.DictCompare(schema_files_path,env_a, env_b )
    dbs = dc.get_databases()
    return render_template("home.html", dbs=dbs,  a_name =dc.a_name, b_name = dc.b_name,form =form )



@app.route('/<startjob>')
@app.route('/', methods=['POST','GET'])
def index(startjob = ''):
    return "<a href='/home'>home</a>"

@app.route('/submit_form', methods=['POST'])
def info():
    form=EnvForm(csrf_enabled=False)
    env_a = request.form['env_a']
    env_b = request.form['env_b']
    dc = sc.DictCompare(schema_files_path, env_a, env_b)
    dbs = dc.get_databases()
    return render_template("home.html", dbs=dbs,  a_name =dc.a_name, b_name = dc.b_name, form=form)



if __name__ == '__main__':
    print "Starting hivediff.py.... "
    app.run(debug=True)
