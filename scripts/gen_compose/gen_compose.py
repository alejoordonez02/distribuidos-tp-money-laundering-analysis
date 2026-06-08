from .src import (
    gen_clients,
    gen_default_filters,
    gen_gateway,
    gen_join,
    gen_rabbitmq,
    gen_uc1,
    gen_uc2,
    gen_uc3,
    gen_uc4,
    gen_uc5,
)

NDEFAULT_FILTERS = 2


def main():
    compose = "services:"
    compose += gen_rabbitmq()
    compose += gen_gateway()
    compose += gen_default_filters(NDEFAULT_FILTERS)
    compose += gen_uc1()
    compose += gen_uc2()
    compose += gen_uc3()
    compose += gen_uc4()
    compose += gen_uc5()
    compose += gen_join()
    compose += gen_clients()

    with open("test.yaml", "w") as f:
        f.write(compose)


if __name__ == "__main__":
    main()
