from app.pipeline import run_all
import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()

result = run_all(args.input, args.output)
print(json.dumps(result, ensure_ascii=False, indent=2))
