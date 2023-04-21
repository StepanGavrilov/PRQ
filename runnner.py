import subprocess

from gevent import subprocess as g_sub

from scripts.log import lloger


class Runner:
    lloger = lloger

    def __init__(
        self,
        entrypoint_path: str,
        return_result: bool = False,
        save_result: bool = False,
        wait_result: bool = False,
    ):
        self.entrypoint_path = entrypoint_path
        self.return_result = return_result
        self.save_result = save_result
        self.wait_result = wait_result

        print(f"Runner INIT: {self.entrypoint_path, self.wait_result}")

    def run(self):
        if not self.wait_result:
            execution_proc: g_sub.Popen = subprocess.Popen(self.entrypoint_path, shell=True)  # type: ignore
        else:
            execution_proc = subprocess.Popen(self.entrypoint_path, shell=True).wait()  # type: ignore
