import shutil
import platform
import os
import subprocess
import threading
import time

class Utils():    
    def get_set_environ_command(self):
        """
        환경변수를 세팅할 때 사용하는 명령어를 가져온다.
        """
        if self.is_windows():
            set_environ_command = "SET"
        else:
            set_environ_command = "export"        
        return set_environ_command

    def get_os(self):
        """
        platform.system() 과 같음
        """
        return platform.system()

    def is_windows(self):
        """
        현재 사용하는 OS가 Windows 인지 확인한다.
        """
        return self.get_os() == "Windows"


    def get_unique_service_path(self, service_base_path, service_name):
        service_path = ""
        for dirpath, dirnames, _ in os.walk(service_base_path):
            if service_name in dirnames:
                service_path = os.path.abspath(dirpath + "/" + service_name)
                break

        if service_path == "":
            raise ValueError(f"{service_base_path} {service_name}, 서비스를 찾지 못했습니다.")

        result = service_path.replace("\\", "/")
        if len(result.split("/")) == 1:
            raise ValueError(f"invalid service {service_name}")

        return result

    def rmtree(self, path):
        try:
            shutil.rmtree(path)
        except FileNotFoundError:
            pass

    def check_output(self, command):
        if isinstance(command, list):
            command = "&&".join(command)

        result = ""
        print("[명령어] " + command)

        process = subprocess.Popen(
            command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        threading.Thread(target=self._print_output,
                         args=(process.stdout, )).start()

        for b_line in iter(process.stderr.readline, b''):
            try:
                line = b_line.decode("utf-8")
            except:
                try:
                    line = b_line.decode("cp949")
                except:
                    line = str(b_line)

            print(line, end="")
            result += line

        return result

    def _print_output(self, out):
        for b_line in iter(out.readline, b''):

            try:
                line = b_line.decode("cp949")
            except:
                try:
                    line = b_line.decode("utf-8")
                except:
                    line = str(b_line)

            print(line, end="")



    def copy_directory(self, from_path, to_path):
        from_base_dir = os.path.abspath(from_path).replace("\\", "/")
        to_base_dir = os.path.abspath(to_path).replace("\\", "/")
        copyed_file_paths = []
        for dirpath, _, filenames in os.walk(from_base_dir):
            for filename in filenames:
                from_file_path = (dirpath + "/" + filename).replace("\\", "/")
                append_file_path = from_file_path.replace(from_base_dir, "")
                to_file_path = to_base_dir + append_file_path
                while True:
                    try:
                        with open(from_file_path, "rb") as fp:
                            readed = fp.read()

                        to_path_dir = os.path.dirname(to_file_path)
                        if not os.path.isdir(to_path_dir):
                            os.makedirs(to_path_dir)

                        with open(to_file_path, "wb") as fp:
                            fp.write(readed)

                    except Exception as e:
                        print(f"삭제 재시도 중입니다. . . {e}")
                        time.sleep(1)
                    else:
                        break
                copyed_file_paths.append(to_file_path)
        

        all_to_file_path = []
        for dirpath, _, filenames in os.walk(to_base_dir):
            for filename in filenames:
                to_file_path = (dirpath + "/" + filename).replace("\\", "/")
                all_to_file_path.append(to_file_path)

        for file_path in list(set(all_to_file_path) - set(copyed_file_paths)):

            while True:
                try:
                    os.unlink(file_path)
                except Exception as e:
                    print(f"삭제 재시도 중입니다. . . {e}")
                    time.sleep(1)
                else:
                    break