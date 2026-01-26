import asyncio
import logging
import sys

from dapla_metadata.standards.standard_validators import check_naming_standard
from dapla_metadata.standards.standard_validators import generate_validation_report

from config.config import settings

# Gjøre at logging vises
logging.basicConfig(
    format="%(levelname)s: %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
    force=True,
)


async def main():
    results = await check_naming_standard(settings.product_root_dir)
    report = generate_validation_report(results)

    # Log the report
    logging.info("\n%s", report)

    violations = [r for r in results if not r.success]
    if not violations:
        print("Everything is OK")
    else:
        for v in violations:
            print(v.file_path)
            print("\t" + "\n\t".join(v.messages))
            print("\t\t" + "\n\t\t".join(v.violations) + "\n")


if __name__ == "__main__":
    asyncio.run(main())
