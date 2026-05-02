
import logging


def remove_pex_arg(argv, script_name):
    args = argv[1:]  # Remove the pex file path

    # Optionally: filter out any pex-injected arguments if needed
    args = [arg for arg in args if "pex" not in arg]

    logging.info(f"Clean args: {args}")

    return args
