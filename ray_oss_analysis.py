#!/usr/bin/env python3
import argparse
import logging
import subprocess
import re
import pandas as pd
import json


logger = logging.getLogger("ray_oss_analysis")

def setup_logger(log_file=None, enable_debug=False):
    logger.setLevel(logging.DEBUG if enable_debug else logging.INFO)
    
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    if log_file:
        file_handler = logging.FileHandler(log_file, mode='w')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.info(f"Logging to file: {log_file}")

    logger.info(f"Log Level Set to {logging.getLevelName(logger.level)}")

def clean_bazel_line(line):
    return re.split(r'\s+(?=@|//)', line.strip(), maxsplit=1)

def isExcludedKind(kind_str):
    kind = kind_str.split(' ')[0]
    non_target_rule_kinds = ["config_setting", "pkg_files", "pkg_zip_impl", "int_flag", "string_flag", "bool_flag", 'bind', "constraint_value", "constraint_setting"]
    python_rule_kinds = ["py_library", "py_binary", "py_test"]

    target_rule_kinds = non_target_rule_kinds + python_rule_kinds
    return kind in target_rule_kinds

def isBuildTool(label):
    build_package_labels = ["bazel_tools", "local_config_python", "cython"]
    return any(build_package_label in label for build_package_label in build_package_labels)


def isOwnCode(label):
    # actualPath = subprocess.run(f"{bazel_command} query --output=location '{label}'", shell=True, capture_output=True, text=True).stdout.strip()
    return label.startswith("//")

def isCppCode(label):
    cExternsions = ['.c', '.cc', '.cpp', '.cxx', '.c++', '.h', '.hpp', '.hxx']
    return any(path in label for path in cExternsions)

def get_bazel_dependencies(package_name):
    bazel_dependencies = []
    package_names = set()
    command = f"{bazel_command} query --output=label_kind 'deps({package_name})'"
    logger.debug(f"Running command: {command}")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    lines = result.stdout.splitlines()
    logger.debug(f"Found {len(lines)} dependencies")
    for line in lines:
        logger.debug(f"Dependency: {line}")
        kind, label = clean_bazel_line(line)
        logger.debug(f"Dependency kind: {kind}, Label: {label}")
        if isExcludedKind(kind) or isBuildTool(label) or isOwnCode(label):
            logger.debug(f"Skipping dependency: {line} because it is a bad kind")
            continue
        elif isCppCode(label):
            bazel_dependencies.append(label)
            package_name = re.search(r'(?:@([^/]+))?//', label).group(1)
            package_names.add(package_name)
    return bazel_dependencies, package_names

def copy_files(file_paths):
    for file_path in file_paths:
        logger.debug(f"Copying file: {file_path}")
        subprocess.run(f"mkdir -p $(dirname {output_folder}/{file_path}) && cp {bazel_output_base}/external/{file_path} {output_folder}/{file_path}", shell=True)

def copy_licenses(package_names):
    for package_name in package_names:
        subprocess.run(f"cp {bazel_output_base}/external/{package_name}/**LICENSE* {output_folder}/{package_name}/", shell=True)
        subprocess.run(f"cp {bazel_output_base}/external/{package_name}/**COPYING* {output_folder}/{package_name}/", shell=True)

def run_askalono(dependencies):
    license_info = []
    askalono_pattern = re.compile(r'^(\/[^\n]+)\nLicense:\s*([^\n]+)\nScore:\s*([0-9.]+)', re.M)
    for dependency in dependencies:
        license_text = subprocess.run(f"askalono crawl {bazel_output_base}/external/{dependency}", capture_output=True, text=True, shell=True).stdout.strip()
        licenses = [
            {"dependency": dependency, "path": m.group(1), "license": m.group(2).strip(), "score": float(m.group(3)), "content": open(m.group(1)).read()}
            for m in askalono_pattern.finditer(license_text)
        ]
        license_info.extend(licenses)
        if not licenses:
            logger.warning(f"No license text found for {dependency}")
            licenses.append({
                "dependency": dependency,
                "path": "unknown",
                "license": "unknown",
                "score": "0.0",
                "content": "unknown"
            })
    return license_info


if __name__ == "__main__":
    global bazel_command
    global bazel_output_base
    global output_folder

    parser = argparse.ArgumentParser(description="OSS Analysis Combo Tool")
    
    parser.add_argument("-o", "--output", help="Output folder path", default="oss_analysis")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("-cmd", "--bazel-cmd", help="Bazel command bazel/bazelisk", default="bazel")
    parser.add_argument("-ws", "--bazel-workspace", help="Bazel workspace", default="./")
    parser.add_argument("-p", "--package", help="Bazel package", default="//:gen_ray_pkg")
    parser.add_argument("--log-file", help="Log file path")
    parser.add_argument("--askalono", action="store_true", help="run askalono scan")

    parser.formatter_class=argparse.RawTextHelpFormatter
    parser.description = """
Ray OSS Analysis Combo Tool - Analyze Ray's open source components
default and mandatory behaviour is to copy the dependencies to the output folder
if --askalono is provided, run askalono scan on the selected dependencies directly from bazel external folder
current status: scans only c libraries
    """
    parser.epilog = """
Examples:
    %(prog)s --input packages.json --output oss_analysis
    %(prog)s --input packages.json --output oss_analysis --askalono
    %(prog)s --input packages.json --verbose
    """

    
    args = parser.parse_args()
    setup_logger(args.log_file, args.verbose)
    bazel_command = args.bazel_cmd
    bazel_output_base = subprocess.run(f"{bazel_command} info output_base", shell=True, capture_output=True, text=True).stdout.strip()
    bazel_workspace = subprocess.run(f"{bazel_command} info workspace", shell=True, capture_output=True, text=True).stdout.strip()

    bazel_dependencies, package_names = get_bazel_dependencies(args.package)
    file_paths = [re.sub(r"^@([^/]+)//(?::)?", r"\1/", dep).replace(":", "/") for dep in bazel_dependencies]

    print("Bazel Dependencies:")
    logger.info(f"Found {len(bazel_dependencies)} dependencies")
    for dependency in bazel_dependencies:
        logger.debug(f"Dependency: {dependency}")

    output_folder = args.output
    copy_files(file_paths)
    copy_licenses(package_names)
    
    if args.askalono:
        askalono_results = run_askalono(package_names)
        with open(f"{output_folder}/output.json", "w") as file:
            json.dump(askalono_results, file, indent=4)
        df = pd.DataFrame(askalono_results)
        # exploded_df = df.explode('licenses', ignore_index=False)
        df.to_excel(f"{output_folder}/askalono_results.xlsx", index=False)

    print("Bazel Install Base Path:")