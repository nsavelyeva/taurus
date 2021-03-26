"""
Copyright 2021 BlazeMeter Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import os
import shutil
import json
from bzt import TaurusConfigError, ToolError
from bzt.engine import HavingInstallableTools
from bzt.modules import ScenarioExecutor, FileLister, SelfDiagnosable
from bzt.modules.console import WidgetProvider, ExecutorWidget
from bzt.modules.aggregator import ResultsReader, ConsolidatingAggregator
from bzt.utils import RESOURCES_DIR, RequiredTool, CALL_PROBLEMS, FileReader, shutdown_process


class K6Executor(ScenarioExecutor, FileLister, WidgetProvider, HavingInstallableTools, SelfDiagnosable):
    def __init__(self):
        super(K6Executor, self).__init__()
        self.output_file = None
        self.log_file = None
        self.script = None
        self.process = None
        self.k6 = None
        self.kpi_file = None
        self.scenario = None

    def prepare(self):
        super(K6Executor, self).prepare()
        self.scenario = self.get_scenario()
        self.install_required_tools()

        self.script = self.get_script_path()
        if not self.script:
            if not self.scenario.get('requests'):
                raise TaurusConfigError("Either 'script' or 'scenario' should be present for K6 executor")
            proto = "grpc" if "procedure" in self.scenario["requests"][0] else "http"  # try-except?
            self.script = os.path.join(self.engine.artifacts_dir, f"k6_script_{proto}.js")
            with open(os.path.join(RESOURCES_DIR, "k6", "templates", f"{proto}.txt"), "r") as f:
                content = f.read()
            if proto == "http":
                requests = self.scenario.get_requests()
                batch_requests = {}
                i = 0
                for request in requests:
                    request_dict = {"method": request.method, "url": request.url}
                    if request.headers:
                        request_dict.update({"params": {"headers": request.headers}})
                    if request.body:
                        request_dict.update({"body": request.body})
                    i += 1
                    batch_requests.update({f"Request {i}": request_dict})
                content = content.replace("REQUESTS", json.dumps(batch_requests, indent=4, sort_keys=True))
            else:  # i.e. grpc
                scenario = self.scenario["requests"][0]
                print(scenario)
                if set(["host", "port", "procedure"]) <= set(scenario.keys()):
                    content = content.replace("HOST", scenario["host"]).replace("PORT", str(scenario["port"]))
                    if scenario.get("protofile"):
                        print(os.path.join(os.path.abspath(os.path.curdir), 'temp', 'k6', scenario["protofile"]))
                        if os.path.isfile(os.path.join(os.path.abspath(os.path.curdir), 'temp', 'k6', scenario["protofile"])):
                            shutil.copy(os.path.join(os.path.curdir, 'temp', 'k6', scenario["protofile"]),
                                            # TODO: analyze where provided YAML was
                                            os.path.join(self.engine.artifacts_dir, scenario["protofile"]))
                        else:
                            raise TaurusConfigError(f"Error accessing protofile. Does it exist?")
                        if os.path.sep in scenario["protofile"]:
                            folder = '"{}"'.format(scenario["protofile"][:scenario["protofile"].rfind(os.path.sep)])
                            protofile = '"{}"'.format(scenario["protofile"][scenario["protofile"].rfind(os.path.sep):])

                        else:
                            folder, protofile = "", scenario["protofile"]
                        content = content.replace("PROTOFILE", 'client.load([{}], "{}");'.format(folder, protofile))
                    else:
                        content = content.replace("PROTOFILE", "")
                    if scenario.get("data"):
                        data = json.dumps(scenario["data"])
                        content = content.replace("REQUEST",
                                                  'client.invoke("{}", {});'.format(scenario["procedure"], data))
                    else:
                        content = content.replace("REQUEST", 'client.invoke("{}")'.format(scenario["procedure"]))
                    if scenario.get("plaintext"):
                        content = content.replace("PLAINTEXT", str(scenario["plaintext"]).lower())
                    else:
                        content = content.replace("PLAINTEXT", "false")
                else:
                    raise TaurusConfigError("For gRPC request at least 'host', 'port', 'procedure' should be provided.")
            with open(self.script, "w") as f:
                f.write(content)

        self.stdout = open(self.engine.create_artifact("k6", ".out"), "w")
        self.stderr = open(self.engine.create_artifact("k6", ".err"), "w")

        self.kpi_file = self.engine.create_artifact("kpi", ".csv")
        self.reader = K6LogReader(self.kpi_file, self.log)
        if isinstance(self.engine.aggregator, ConsolidatingAggregator):
            self.engine.aggregator.add_underling(self.reader)

    def startup(self):
        cmdline = [self.k6.tool_name, "run",
                   "--out", f"csv={self.kpi_file}",
                   "--summary-trend-stats", "min,avg,max,p(50),p(90),p(95),p(99),p(99.9),p(100)",
                   "--summary-export", os.path.join(self.engine.artifacts_dir, "k6-summary.json")]

        load = self.get_load()
        if load.throughput:
            with open(os.path.join(RESOURCES_DIR, "k6", "templates", f"options.txt"), "r") as f:
                options = f.read()
            with open(self.script, "a") as f:
                f.write(options)
            cmdline += ['--env', 'arrival_rate=' + str(load.throughput)]
            cmdline += ['--env', 'initialized_vus=' + str(load.concurrency or load.throughput // 10 or 1)]
            if load.hold:
                cmdline += ['--env', 'duration=' + str(int(load.hold)) + "s"]
            # iterations ignored
        else:
            if load.concurrency:
                cmdline += ['--vus', str(load.concurrency)]
            if load.hold:
                cmdline += ['--duration', str(int(load.hold)) + "s"]
            if load.iterations:
                cmdline += ['--iterations', str(load.iterations)]

        cmdline += [self.script]
        self.process = self._execute(cmdline)

    def get_widget(self):
        if not self.widget:
            label = "%s" % self
            self.widget = ExecutorWidget(self, "K6: " + label.split('/')[1])
        return self.widget

    def check(self):
        retcode = self.process.poll()
        if retcode is not None:
            ToolError(f"K6 tool exited with non-zero code: {retcode}")
            return True
        return False

    def shutdown(self):
        shutdown_process(self.process, self.log)

    def post_process(self):
        if self.kpi_file:
            self.engine.existing_artifact(self.kpi_file)
        super(K6Executor, self).post_process()

    def install_required_tools(self):
        self.k6 = self._get_tool(K6, config=self.settings)
        self.k6.tool_name = self.k6.tool_name.lower()
        if not self.k6.check_if_installed():
            self.k6.install()


class K6LogReader(ResultsReader):
    def __init__(self, filename, parent_logger):
        super(K6LogReader, self).__init__()
        self.log = parent_logger.getChild(self.__class__.__name__)
        self.file = FileReader(filename=filename, parent_logger=self.log)
        self.data = {'timestamp': [], 'label': [], 'r_code': [], 'error_msg': [], 'http_req_duration': [],
                     'http_req_connecting': [], 'http_req_tls_handshaking': [], 'http_req_waiting': [], 'vus': [],
                     'data_received': []}

    def _read(self, last_pass=False):
        self.lines = list(self.file.get_lines(size=1024 * 1024, last_pass=last_pass))

        for line in self.lines:
            if line.startswith("http_req_duration") or line.startswith("grpc_req_duration"):
                self.data['timestamp'].append(int(line.split(',')[1]))
                self.data['label'].append(line.split(',')[8])
                self.data['r_code'].append(line.split(',')[12])
                self.data['error_msg'].append(line.split(',')[4])
                self.data['http_req_duration'].append(float(line.split(',')[2]))
            elif line.startswith("http_req_connecting"):
                self.data['http_req_connecting'].append(float(line.split(',')[2]))
            elif line.startswith("http_req_tls_handshaking"):
                self.data['http_req_tls_handshaking'].append(float(line.split(',')[2]))
            elif line.startswith("http_req_waiting"):
                self.data['http_req_waiting'].append(float(line.split(',')[2]))
            elif line.startswith("vus") and not line.startswith("vus_max"):
                self.data['vus'].append(int(float(line.split(',')[2])))
            elif line.startswith("data_received"):
                self.data['data_received'].append(float(line.split(',')[2]))

            if self.data['vus'] and len(self.data['data_received']) >= self.data['vus'][0] and \
                    len(self.data['http_req_waiting']) >= self.data['vus'][0]:
                for i in range(self.data['vus'][0]):
                    kpi_set = (
                        self.data['timestamp'][0],
                        self.data['label'][0],
                        self.data['vus'][0],
                        self.data['http_req_duration'][0] / 1000,
                        (self.data['http_req_connecting'][0] + self.data['http_req_tls_handshaking'][0]) / 1000,
                        self.data['http_req_waiting'][0] / 1000,
                        self.data['r_code'][0],
                        None if not self.data['error_msg'][0] else self.data['error_msg'][0],
                        '',
                        self.data['data_received'][0])

                    for key in self.data.keys():
                        if key != 'vus':
                            self.data[key].pop(0)

                    yield kpi_set

                self.data['vus'].pop(0)


class K6(RequiredTool):
    def __init__(self, config=None, **kwargs):
        super(K6, self).__init__(installable=False, tool_path="/usr/local/bin/k6", **kwargs)

    def check_if_installed(self):
        self.log.debug('Checking K6 Framework: %s' % self.tool_path)
        try:
            out, err = self.call(['k6', 'version'])
        except CALL_PROBLEMS as exc:
            self.log.warning("%s check failed: %s", self.tool_name, exc)
            return False

        if err:
            out += err
        self.log.debug("K6 output: %s", out)
        return True
