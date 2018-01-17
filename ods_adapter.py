from bosh_api import *
import yaml, json
from jsonpath_ng.ext import parse

class OdsAdapter():
    _env = None
    _manifest = None
    _name = None
    _dname = None
    _job = None
    _must_alivejob = []
    _render_manifest = []
    _info_fetcher = {}
    _wf_def = None
    
    def __init__(self, id, env):
        if not isinstance(env, BoshEnv):
            raise TypeError("%s.__init__(): env should be instance of BoshEnv"%self.__class__.__name__)
        self._name = "%s-%s"%(self._dname, id)
        self._env = env
        if self._wf_def is None:
            self._def_workflow()
        self.gen_manifest()
        self._validate()
    def gen_manifest(self):
        discard = parse("$.name").update(self._manifest, self._name)
    def _validate(self):
        checkers = [("_name", str),
                    ("_dname", str),
                    ("_env", BoshEnv),
                    ("_job", str),
                    ("_must_alivejob", list),
                    ("_manifest", dict),
                    ("_render_rules", list),
                    ("_info_fetcher", dict),
                    ("_wf_def", dict)]
        failed = [(k, t) for k, t in checkers
                  if not isinstance(getattr(self, k), t)]
        if len(failed) > 0:
            err = ["%s should be instance of %s"%(k, str(t)) for k,t in failed]
            errstr = "\n".join(err)
            raise TypeError("%s.__init__(): %s"%(self.__class__.__name__, errstr))
    def __repr__(self):
        return "<%s %s>"%(self.__class__.__name__, self._name)
    def fetch_info(self):
        m = self._env.deployment_by_name(self._name)
        manifest = yaml.load(m.manifest)
        r = dict([(key, [i.value for i in parse(path).find(manifest)])
         for key, path in self._info_fetcher.items() ])
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
    @staticmethod
    def password_generator(k):
        import string, random
        p = lambda: "".join(random.choices(string.ascii_letters+string.digits, k=k))
        return p
    def _render_manifest(self):
        init = self._manifest
        for p, v in self._render_rules:
            if callable(v):
                init = parse(p).update(init, v())
            else:
                init = parse(p).update(init, v)
        return json.dumps(init)
    def calldeploy(self, task_id = None):
        if task_id is not None:
            return self.checktask(task_id, "deploy")
        if self._manifest is None:
            raise TypeError("%s.calldeploy: manifest is not specified"%self.__class__.__name__)
        t = self._env.deploy(self._render_manifest())
        return self.calldeploy(t.id)
    def runerrand(self, errand, step, task_id = None):
        if task_id is not None:
            return self.checktask(task_id, step)
        t = self._env.run_errand(self._name, errand)
        return self.runerrand(errand, step, t.id)
    def checktask(self, task_id, step):
        jobstatemap={"queued":"pollagain",
                      'processing':"pollagain",
                      'cancelling':'pollagain',
                      'error': 'error'}
        t = self._env.task_by_id(task_id)
        return "%s_%s"%(step, jobstatemap.get(t.state, 'done')), task_id
    def callinstancestates(self, task_id):
        if task_id is not None:
            r, t = self.checktask(task_id, "states")
            if r == "states_done":
                return self.checkstate(task_id)
            return r,t
        t = self._env.instance_states(self._name)
        return self.callinstancestates(t.id)
    def checkstate(self, task_id):
        t = self._env.task_by_id(task_id)
        t.set_result_class(BoshInstanceState)
        res = t.result()
        if all(i.job_state == 'running' for i in res if i.job_name in self._must_alivejob):
            return 'states_done',task_id
        else:
            return 'states_error', task_id
    def calldelete(self, task_id):
        if task_id is not None:
            return self.checktask(task_id, "delete")
        try:
            t = self._env.delete_deploy(self._name)
        except BoshRequestError as e:
            if e.code == 404:
                return "delete_done", None
            raise e
        return self.calldelete(t.id)
    def _def_workflow(self):
        self._wf_def = {"deploy": "deploy_pollagain",
                        "deploy_pollagain": self.calldeploy,
                        "deploy_done": "states_pollagain",
                        "states_pollagain": self.callinstancestates,
                        "states_done": "deploy_finish",
                        "deploy_finish": "finish",
                        "delete": "delete_pollagain",
                        "delete_pollagain": self.calldelete,
                        "delete_done": "finish",
                        "finish": "finish"
        }
    def _insert_workflow(self, after, wf):
        if after not in self._wf_def or after not in wf:
            raise TypeError("_insert_workflow: %s must exists in both `_wf_def' and `wf'"%after)
        end = self._wf_def[after]
        if not isinstance(end, str):
            raise TypeError("_insert_workflow: right side of %s is not symbol, stop!"%after)
        if not end in wf.values():
            raise TypeError("_insert_workflow: %s does not exists in `wf'"%end)
        for k,v in wf.items():
            self._wf_def[k] = v
    def _workflow(self, pre, t):
        if pre not in self._wf_def:
            return 'error', None
        if pre in ('finish',):
            return 'finish', None
        f = self._wf_def[pre]
        if callable(f):
            return f(t)
        return self._workflow(f, None)
    def workflow(self, pre, t):
        return self._workflow(pre, t)
