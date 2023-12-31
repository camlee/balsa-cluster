import trio

from cluster import Cluster

class DispatchCluster(Cluster):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.registered_methods = {} # Key is the type, value is the method

        for name in dir(self):
            method = getattr(self, name)
            if hasattr(method, "_handle_types"):
                for type in method._handle_types:
                    methods = self.registered_methods.setdefault(type, [])
                    methods.append(method)

    async def dispatch_messages(self):
        async for message in self.messages:
            if not isinstance(message.obj, dict):
                print(f"Warning: received a message that's not a dict: '{message.obj}'")
                continue
            message_type = message.obj.get("type")

            methods = self.registered_methods.get(message_type, [])

            # print(f"Dispatching {message_type} to {methods}")

            if methods:
                async with trio.open_nursery() as nursery:
                    for method in methods:
                        nursery.start_soon(method, message)
            else:
                print(f"No handler for message {message.obj} from {message.connection}.")

    async def run(self):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(super().run)
            nursery.start_soon(self.dispatch_messages)


def handler(type):
    """
    Used to indicate which methods should be called for which
    message types. This function decorator simply records this on the
    function and the DispatchCluster class finds functions using this metadata.

    Example Usage:

    class MyCluster(DispatchCluster):
        @handler("hello")
        def handle_hello(self, message):
            print("Received a message of type hello!")
    """
    def decorator(func):
        if not hasattr(func, "_handle_types"):
            func._handle_types = []
        func._handle_types.append(type)
        return func
    return decorator