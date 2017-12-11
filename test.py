import time, os
import ods_adapter
from bosh_api import *

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

def main():
    dname = "learn-bosh-2"
    env=BoshEnv("192.168.50.6", os.getenv("BOSH_CLIENT"), os.getenv("BOSH_CLIENT_SECRET"),
                cacert=os.getenv("BOSH_CA_CERT"))
    adp = ods_adapter.OdsAdapter("learn-bosh-2", env, "app", ["app"], d)

    n, t = adp.workflow("delete", None)
    while True:
        print( n, t)
        if n == "finish" or n == "error":
            break
        n, t = adp.workflow(n, t)
        time.sleep(30)

    n, t = adp.workflow("deploy", None)
    while True:
        print( n, t)
        if n == "finish" or n == "error":
            break
        n, t = adp.workflow(n, t)
        time.sleep(30)
    print(adp.get_creds())
if __name__ == '__main__':
    main()
