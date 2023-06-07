from time import perf_counter


# Context manager to print time elapsed in a block of code
class catchtime:
    def __init__(self, description: str):
        self.description = description

    def __enter__(self):
        self.time = perf_counter()
        return self

    def __exit__(self, type, value, traceback):
        self.time = perf_counter() - self.time
        self.readout = f'Time to {self.description}: {self.time:.3f} seconds'
        print(self.readout)
