import argparse
import subprocess

import ver_cli


def main(cnf, args: argparse.Namespace):
    ver = ver_cli.VerCLI()

    if getattr(args, "build-profile"):
        sources = getattr(args, "data")
        output = getattr(args, "profile")
        store_type = getattr(args, "profile-store-type", 1)
        ver.profile(sources, output, store_type)
    
    if getattr(args, "build-dindex"):
        force = getattr(args, "force-build")
        sources = getattr(args, "profile")
        ver.build_dindex(sources, force)
    else:
        ver.load_dindex()

    if getattr(args, "test"):
        subprocess.call(["python", "ver_test.py"])

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-profile", dest="build-profile", default=False, action="store_true",
                        help="build profile")
    parser.add_argument("--build-dindex", dest="build-dindex", default=False, action="store_true",
                        help="build dindex")
    parser.add_argument("--test", dest="test", default=False, action="store_true",
                        help="enable testing mode")
    parser.add_argument("--data", type=str, default="demo_dataset/",
                        help="data loaction")
    parser.add_argument("--profile", type=str, default="ddprofiler/output_profiles_json/",
                        help="profiles loaction")
    parser.add_argument("--profile-store-type", dest="profile-store-type", default=1, type=int,
                        help="type of profile store")
    parser.add_argument("--force-build", dest="force-build", default=False, action="store_true",
                        help="force build")
    return parser

if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    main(None, args)