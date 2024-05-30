from IPython.core.magic import Magics, magics_class, line_magic
from IPython import get_ipython

from keplerviz.keplerviz import load_ais_csv


@magics_class
class CustomMagics(Magics):
    @line_magic
    def ais(self, line):
        splits = line.split(' ')
        if splits[0] == 'load':
            r = load_ais_csv(splits[1])
            return r
        else:
            return f"Unknown command {line}"


def load_ipython_extension(ipython):
    ipython.register_magics(CustomMagics)
