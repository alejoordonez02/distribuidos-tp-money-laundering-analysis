import sys

from .src import (
    gen_chaos,
    gen_clients,
    gen_default_filters,
    gen_gateway,
    gen_join,
    gen_rabbitmq,
    gen_supervisors,
    gen_uc1,
    gen_uc2,
    gen_uc3,
    gen_uc4,
    gen_uc5,
)


def main():
    args = sys.argv

    if len(args) != 2:
        sys.exit(1)

    filename = args[1]

    # Build the node services first so every supervised node has registered its
    # name (via supervisor_env) before gen_supervisors reads the expected set.
    gateway = gen_gateway()
    default_filters = gen_default_filters()
    uc1, uc2, uc3, uc4, uc5 = gen_uc1(), gen_uc2(), gen_uc3(), gen_uc4(), gen_uc5()
    join = gen_join()
    clients = gen_clients()
    # gen_supervisors must run last: it injects EXPECTED_NODES from the now-complete set.
    supervisors = gen_supervisors()

    compose = "services:"
    compose += gen_rabbitmq()
    compose += supervisors
    compose += gateway
    compose += default_filters
    compose += uc1
    compose += uc2
    compose += uc3
    compose += uc4
    compose += uc5
    compose += join
    compose += gen_chaos()
    compose += clients

    with open(filename, "w") as f:
        f.write(compose)


if __name__ == "__main__":
    main()
