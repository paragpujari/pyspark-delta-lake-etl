from src.pipeline.run_pipeline import run

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--generate-only", action="store_true")
    args = parser.parse_args()
    run(generate_only=args.generate_only)
