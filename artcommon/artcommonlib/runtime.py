from abc import ABC, abstractmethod

# an abstract class intended to be used by anything looking for config specific to a single
# ocp-build-data group.


class GroupRuntime(ABC):

    # right now we're only interested in guaranteeing this has a group_config property
    @property
    @abstractmethod
    def group_config(self):
        pass
