from bosh_api import *
import yaml, json
from jsonpath_ng import jsonpath, parse

class OdsAdapter():
    _env = None
    _manifest = None
    _name = None
    _job = None
    def __init__(self, name, env, job, must_alivejob = [], manifest=None, render_rules=[], info_fetcher = {}):
        if not isinstance(env, BoshEnv):
            raise TypeError("%s.__init__(): env should be instance of BoshEnv"%self.__class__.__name__)
        self._name = name
        self._job = job
        self._render_rules = render_rules
        self._env = env
        self._must_alivejob = must_alivejob
        self._def_workflow()
        self._info_fetcher = info_fetcher
        if manifest is not None:
            self._manifest = yaml.load(manifest)
    def __repr__(self):
        return "<%s %s>"%(self.__class__.__name__, self._name)
    def fetch_info(self):
        m = self._env.deployment_by_name(self._name)
        manifest = yaml.load(m.manifest)
        r = [(key, [i.value for i in parse(path).find(manifest)])
         for key, path in self._info_fetcher.items() ]
        return r
    def get_creds(self):
        cred = {"credentials":
                {"cluster":
                 [{"host": i.ips, "port": 8080}
                  for i in self._env.instances(self._name)
                     if i.job == self._job]
                }
        }
        return cred
    def _render_manifest(self):
        init = self._manifest
        for p, v in self._render_rules:
            init = parse(p).update(init, v)
        return json.dumps(init)
    def calldeploy(self, task_id = None):
        if task_id is not None:
            r = self.checktask(task_id)
            if r == 'done':
                return 'deploy_%s'%r, None
            return "deploy_%s"%r, task_id
        if self._manifest is None:
            raise TypeError("%s.calldeploy: manifest is not specified"%self.__class__.__name__)
        t = self._env.deploy(self._render_manifest())
        return self.calldeploy(t.id)
    def checktask(self, task_id):
        jobstatemap={"queued":"poolagain",
                      'processing':"poolagain",
                      'cancelling':'poolagain',
                      'error': 'error'}
        t = self._env.task_by_id(task_id)
        return jobstatemap.get(t.state, 'done')
    def callinstancestates(self, task_id):
        if task_id is not None:
            r = self.checktask(task_id)
            return "states_%s"%r, task_id
        t = self._env.instance_states(self._name)
        return self.callinstancestates(t.id)
    def checkstate(self, task_id):
        t = self._env.task_by_id(task_id)
        t.set_result_class(BoshInstanceState)
        res = t.result()
        if all(i.job_state == 'running' for i in res if i.job_name in self._must_alivejob):
            return 'deploy_finish',task_id
        else:
            return 'deploy_error', task_id
    def calldelete(self, task_id):
        if task_id is not None:
            r = self.checktask(task_id)
            if r == 'done':
                return 'delete_done', None
            return 'delete_%s'%r, task_id
        try:
            t = self._env.delete_deploy(self._name)
        except BoshRequestError as e:
            if e.code == 404:
                return "delete_done", None
            raise e
        return self.calldelete(t.id)
    def _def_workflow(self):
        self._wf_def = {"deploy": self.calldeploy,
                        "deploy_poolagain": self.calldeploy,
                        "deploy_done": self.callinstancestates,
                        "states_poolagain": self.callinstancestates,
                        "states_done": self.checkstate,
                        "deploy_finish": "finish",
                        "delete": self.calldelete,
                        "delete_poolagain": self.calldelete,
                        "delete_done": "finish",
                        "finish": "finish"
        }
    def _workflow(self, pre, t):
        if pre not in self._wf_def:
            return 'error', None
        f = self._wf_def[pre]
        if callable(f):
            return f(t)
        return f, None
    def workflow(self, pre, t):
        return self._workflow(pre, t)
