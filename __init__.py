import shutil
import glob
import re
from pprint import pprint
import json
import time
import os
import yaml
import sys
import importlib
import boto3
from . import utils

class AWSLambda():
    def __init__(self, bucket_name, lambda_service_dir, environ={}, aws_access_key_id=None, aws_secret_access_key=None, region_name=None, stack_prefix="E"):
        self._environ = environ
        self._stack_prefix = stack_prefix
        self._bucket_name = bucket_name
        self._lambda_service_dir = lambda_service_dir

        self._lambda_client = boto3.client("lambda",
                                           aws_access_key_id=aws_access_key_id,
                                           aws_secret_access_key=aws_secret_access_key,
                                           region_name=region_name)

        self._s3_client = boto3.client("s3",
                                       aws_access_key_id=aws_access_key_id,
                                       aws_secret_access_key=aws_secret_access_key,
                                       region_name=region_name)
        self._utils = utils.Utils()
        


    def get_export_env_string_list(self):
        result = []

        set_env_command = self._utils.get_set_environ_command()

        for env_name in self._environ:
            env_value = self._environ[env_name]
            result.append(f"{set_env_command} {env_name}={env_value}")

        return result

    def get_service_path(self, service_name):
        return self._utils.get_unique_service_path(self._lambda_service_dir, service_name)

    def get_layer_name(self, service_name):
        layer_name = self.get_service_settings(
            service_name).get("layer_name", "common")
        return "common" if layer_name == "" else layer_name

    def compile_template(self, service_name):
        service_path = self.get_service_path(service_name)

        result = self._lambda_client.list_layer_versions(
            LayerName=self.get_layer_name(service_name))
        common_layer_arn = result["LayerVersions"][0]["LayerVersionArn"]
        service_template_path = service_path + "/template.yaml"

        with open(self.get_template_path(), "r", encoding="utf-8") as fp:
            readed = fp.read()
            readed = readed.replace("{{Description}}", service_name)
            readed = readed.replace("{{FunctionName}}", service_name)
            readed = readed.replace("{{CodeUri}}", service_path)
            readed = readed.replace("{{CommonLayerArn}}", common_layer_arn)

            parsed_readed = yaml.full_load(readed)
            parsed_readed["Resources"][service_name]["Properties"]["Environment"]["Variables"] = self._environ

        if os.path.isfile(service_template_path):
            with open(service_template_path, "r", encoding="utf-8") as fp:
                readed = fp.read()
                readed = readed.replace("{{FunctionName}}", service_name)
                readed = readed.replace("{{BucketName}}", self._bucket_name)
                user_template = yaml.full_load(readed)
                for key in user_template:
                    value = user_template[key]
                    last = parsed_readed
                    splited = key.split(".")
                    for splited_key in splited[:-1]:
                        if splited_key not in last:
                            last[splited_key] = {}

                        last = last[splited_key]

                    last[splited[-1]] = value

        with open(self.get_temp_path() + "/template.yaml", "w", encoding="utf-8") as fp:
            dumped = yaml.dump(parsed_readed)
            for raw_ref_str in re.findall(r"'!Ref.*?'", dumped):
                dumped = dumped.replace(raw_ref_str, raw_ref_str[1:-1])
            fp.write(dumped)

    def is_valid_service(self, service_name):
        service_path = self.get_service_path(service_name)
        if not os.path.isdir(service_path):
            raise ValueError(f"존재하지 않는 서비스 이름입니다. 서비스 이름은 {service_name} 입니다.")

    def deploy_layers(self, service_name):
        print("Layer 를 배포하는 중입니다.")
        self.is_valid_service(service_name)

        self._utils.copy_directory(self._utils.get_path(
            "resources_general_layers"), self.get_service_path(service_name) + "/layers")

        print("Layer 를 배포했습니다.")

    def get_template_path(self):
        return self._utils.get_path("resources_aws_lambda") + "/others/template.yaml"

    def get_requirements_path(self):
        return self._utils.get_path("resources_aws_lambda") + "/others/requirements"

    def get_temp_path(self):
        path = self._utils.get_path("resources_temp") + "/aws_lambda"
        self._utils.mkdir(path)
        return path

    def get_create_path(self):
        return self._utils.get_path("resources_aws_lambda") + "/create"

    def get_create_service_path(self, base_dir, service_name):
        return self._utils.get_path("service_aws_lambda") + "/" + base_dir + "/" + service_name

    def get_service_settings(self, service_name):
        service_path = self.get_service_path(service_name)
        service_template_path = service_path + "/settings.json"
        with open(service_template_path, "r", encoding="utf-8") as fp:
            settings = json.loads(fp.read())
        return settings

    def create_service(self, base_dir, service_name):
        print(f"{service_name} 서비스를 생성하는 중입니다.")

        try:
            self.is_valid_service(service_name)
        except:
            pass
        else:
            raise ValueError(f"The {service_name} service is already exists.")

        to_path = self.get_create_service_path(base_dir, service_name)
        # print(self.get_create_path(), to_path)
        self._utils.copy_directory(self.get_create_path(), to_path)

        with open(to_path + "/settings.json", "r", encoding="utf-8") as fp:
            settings = json.loads(fp.read())

        settings["name"] = service_name
        with open(to_path + "/settings.json", "w", encoding="utf-8") as fp:
            fp.write(json.dumps(settings, ensure_ascii=False, indent=4))

        self.deploy_layers(service_name)

        print(f"{service_name} 서비스가 생성되었습니다.")

    def deploy_common_lambda_layer_all(self):

        for *_, files in os.walk(self.get_requirements_path()):
            for filename in files:
                layer_name, _ = os.path.splitext(filename)
                self.deploy_common_lambda_layer(layer_name)

    def deploy_common_lambda_layer(self, layer_name):
        print("람다 레이어를 배포하는 중입니다.")

        with open(f"{self.get_requirements_path()}/{layer_name}.txt", "r") as fp:

            default_service_requirements = fp.read().split("\n")
            lambda_layers_path = self.get_temp_path() + "/lambda_layers"
            self._utils.rmtree(lambda_layers_path + "/space/python")
            os.makedirs(lambda_layers_path + "/space/python", exist_ok=True)

            for package_name in default_service_requirements:
                self._utils.check_output(
                    [f"cd {lambda_layers_path}", f"python3 -m pip install -t ./space/python/ {package_name}"])

            trashnames = ["*.pyc", "*.egg-info", "pyc/**"]
            deleted_dirs = []
            deleted_files = []
            for trash in trashnames:
                paths = glob.glob(lambda_layers_path +
                                  "/space/**/" + trash, recursive=True)
                for path in paths:
                    if os.path.isdir(path):

                        deleted_dirs.append(self._utils.rmtree(path))
                    if os.path.isfile(path):
                        deleted_files.append(os.unlink(path))

            print(
                f"file: {len(deleted_files)}, directory: {len(deleted_dirs)} deleted.")
            time.sleep(1)

            self._utils.check_output(
                [f"cd {lambda_layers_path}/space", f"zip -r ../{layer_name}.zip *"])
            self.deploy_lambda_layer_worker(
                layer_name, lambda_layers_path + f"/{layer_name}.zip")

        print("람다 레이어 배포를 완료했습니다.")

    def deploy_lambda_layer_worker(self, LayerName, zip_path):
        lambda_s3_layer_dir = self._utils.info["lambda_s3_layer_dir"]
        s3_key = lambda_s3_layer_dir + f'/{LayerName}/{LayerName}.zip'

        self.s3_client.upload_file(zip_path, self._bucket_name, s3_key)
        lambda_layer_published = self._lambda_client.publish_layer_version(LayerName=f'{LayerName}', Description=f'{LayerName}', Content={
            'S3Bucket': self._bucket_name, 'S3Key': s3_key}, CompatibleRuntimes=['python3.6', 'python3.7'], LicenseInfo='')

        print(lambda_layer_published)
        return lambda_layer_published

    def deploy(self, service_name):

        self.deploy_layers(service_name)

        self.compile_template(service_name)
        start = time.time()
        print("디플로이를 시작했습니다.")

        commands = ["cd " + self.get_temp_path(), "sam build --use-container",
                    f"sam package --output-template-file packaged.yaml --s3-bucket {self._bucket_name}",
                    f"sam deploy --template-file packaged.yaml --region ap-northeast-2 --capabilities CAPABILITY_IAM --stack-name {self._stack_prefix}-{service_name}"]

        self._utils.check_output("&&".join(commands))
        print(time.time() - start)
        print("디플로이를 완료했습니다.")

    def deploy_all(self, include):

        for service_name in self._utils.get_all_service_names("lambda", include):
            self.deploy(service_name)

    def local_test(self, service_name, pytest):
        self.deploy_layers(service_name)

        start = time.time()
        print("테스트를 시작했습니다.")

        if pytest:
            command = "python3 -m pytest -s test.py"
        else:
            command = "python3 test.py"

        self._utils.check_output([f"cd {self.get_service_path(service_name)}"] +
                                self.get_export_env_string_list() + [command])

        print(time.time() - start)
        print("테스트를 완료했습니다.")

    def test_all(self, include):
        for service_name in self._utils.get_all_service_names("lambda", include):
            self.local_test(service_name, True)

    def work(self, job, args):
        if job == "create":
            self.create_service(args.base_dir, args.service_name)
        elif job == "deploy":
            self._utils.git_push(self._utils.get_path("service_aws_lambda"))
            self.deploy(args.service_name)
        elif job == "deploy-layers":
            self.deploy_layers(args.service_name)
        elif job == "deploy-common-layer":
            self.deploy_common_lambda_layer(args.layer_name)
        elif job == "deploy-common-layer-all":
            self.deploy_common_lambda_layer_all()
        elif job == "pytest":
            self.local_test(args.service_name, pytest=True)
        elif job == "test":
            self.local_test(args.service_name, pytest=False)
        elif job == "git-push":
            self._utils.git_push(self._utils.get_path("service_aws_lambda"))
        elif job == "deploy-all":
            self._utils.git_push(self._utils.get_path("service_aws_lambda"))
            self.deploy_all(args.include)
        elif job == "test-all":
            self.test_all(args.include)
        else:
            raise ValueError(f"invalid job {job}")
