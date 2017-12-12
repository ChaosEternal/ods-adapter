import time, os
import ods_adapter
from bosh_api import *
import yaml
from jsonpath_ng.ext import parse

d = """---
name: learn-bosh-2

releases:
- name: learn-bosh
  version: "0+dev.5"
- name: syslog
  version: 11

update:
  canaries: 1
  max_in_flight: 2
  canary_watch_time: 1000-30000
  update_watch_time: 1000-30000

instance_groups:
- name: app
  azs:
  - z1
  instances: 1
  vm_type: minimal
  stemcell: default
  update:
    max_in_flight: 1
    serial: true
  networks:
  - name: default
  jobs:
  - name: app
    release: learn-bosh
    properties:
      port: 8888
      password: 123444
  - name: router
    release: learn-bosh
    properties:
      port: 8080
      servers: ["http://10.244.0.20:8888"]
  - name: syslog_forwarder
    release: syslog
    consumes:
      syslog_storer: {from: primary_syslog_storer}
- name: syslog_storer_primary
  azs:
  - z1
  jobs:
  - name: syslog_storer
    release: syslog
    provides:
      syslog_storer: {as: primary_syslog_storer}
  instances: 1
  vm_type: minimal
  persisitent_disk_type: 1GB
  stemcell: default
  networks:
  - name: default
  properties:
    syslog:
      transport: tcp
stemcells:
- alias: default
  os: ubuntu-trusty
  version: "3468.5"
"""

class SampleOdsAdapter(ods_adapter.OdsAdapter):
    _manifest = yaml.load(d)
    _job = 'app'
    _must_alivejob = ['app']
    _render_rules = [("$.instance_groups[?name=app]..properties.password", "adadasdda")]
    _info_fetcher = {"password":"$.instance_groups[?name=app]..jobs[?name=app].properties.password",
                     "port":"$.instance_groups[?name=app]..jobs[?name=app].properties.port"}
    def __init__(self, deploy_id, env):
        self._env = env
        name_in_manifest = parse("$.name").find(self._manifest)[0].value
        base_name, rest = name_in_manifest.rsplit("-",1)
        self._name = "%s-%s"%(base_name, deploy_id)
        discard = parse("$.name").update(self._manifest, self._name)
        self._def_workflow()
        self._validate()
    def get_creds(self):
        info = self.fetch_info()
        cred = {"credentials":
                {"cluster":
                 [{"host": i.ips, "port": info["port"][0]}
                  for i in self._env.instances(self._name)
                     if i.job == self._job],
                 "password": str(info["password"][0])
                }
        }
        return cred
    def callerrand(self, task_id):
        return self.runerrand('app', 'errand0', task_id)
    def _def_workflow(self):
        self._wf_def = {"deploy": "deploy_pollagain",
                        "deploy_pollagain": self.calldeploy,
                        "deploy_done": "states_pollagain",
                        "states_pollagain": self.callinstancestates,
                        "states_done": self.checkstate,
                        "deploy_finish": "errand0",
                        "errand0": "errand0_pollagain",
                        "errand0_pollagain": self.callerrand,
                        "errand0_done": "finish",
                        "delete": "delete_pollagain",
                        "delete_pollagain": self.calldelete,
                        "delete_done": "finish",
                        "finish": "finish"
        }

        
def main():
    dname = "learn-bosh-2"
    env=BoshEnv("192.168.50.6", os.getenv("BOSH_CLIENT"), os.getenv("BOSH_CLIENT_SECRET"),
                cacert=os.getenv("BOSH_CA_CERT"))
    adp = SampleOdsAdapter("a1231312", env)
    
    n, t = adp.workflow("delete", None)
    while True:
        print( n, t)
        if n == "finish" or n == "error":
            break
        n, t = adp.workflow(n, t)
        time.sleep(3)

    n, t = adp.workflow("deploy", None)
    while True:
        print( n, t)
        if n == "finish" or n == "error":
            break
        n, t = adp.workflow(n, t)
        time.sleep(3)
    print(adp.get_creds())
    print(adp.fetch_info())
if __name__ == '__main__':
    main()
