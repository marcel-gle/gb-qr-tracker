import json
import subprocess
import shlex

PROJECT = "gb-qr-tracker-dev"
TARGET_DB = "test"  # <- change if needed
FILE = "firestore_indexes/composite-indexes.json"


def get_collection_group(name: str) -> str:
    """
    Extract collection group from index name, e.g.:
    projects/.../collectionGroups/hits/indexes/....
    -> "hits"
    """
    try:
        return name.split("/collectionGroups/")[1].split("/")[0]
    except (IndexError, AttributeError):
        raise ValueError(f"Could not parse collectionGroup from name: {name}")


def main():
    with open(FILE, "r") as f:
        indexes = json.load(f)

    for idx in indexes:
        name = idx.get("name", "")
        collection_group = get_collection_group(name)
        query_scope = idx.get("queryScope", "COLLECTION")
        fields = idx["fields"]

        field_args = []
        for fdef in fields:
            field_path = fdef["fieldPath"]
            if "order" in fdef:
                # gcloud accepts lower-case "ascending"/"descending"
                order = fdef["order"].lower()
                field_args.append(f"field-path={field_path},order={order}")
            elif "arrayConfig" in fdef:
                # Firestore only supports CONTAINS for arrays
                field_args.append(f"field-path={field_path},array-config=contains")
            else:
                raise ValueError(f"Field def has neither order nor arrayConfig: {fdef}")

        cmd = [
            "gcloud", "firestore", "indexes", "composite", "create",
            f"--project={PROJECT}",
            f"--database={TARGET_DB}",
            f"--collection-group={collection_group}",
            f"--query-scope={query_scope}",
        ]

        for fa in field_args:
            cmd.append("--field-config")
            cmd.append(fa)

        print("\nRunning:")
        print(" ".join(shlex.quote(c) for c in cmd))

        # don't use check=True so we can handle ALREADY_EXISTS gracefully
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            stderr = result.stderr or ""
            if "ALREADY_EXISTS" in stderr:
                print("→ Index already exists, skipping.")
                continue
            else:
                print("Command failed with stderr:")
                print(stderr)
                # re-raise as error so you notice unexpected problems
                raise subprocess.CalledProcessError(
                    result.returncode, cmd, output=result.stdout, stderr=result.stderr
                )

        print("→ Index created.")

    print("\nAll composite indexes processed for database:", TARGET_DB)


if __name__ == "__main__":
    main()
