from typing import List


def find_subclasses(cls: type) -> List[type]:
    subclasses = cls.__subclasses__()
    all_subclasses = [cls]
    while subclasses:
        subclass = subclasses.pop()
        subclasses.extend(subclass.__subclasses__())
        if subclass.enable_autoselect:
            all_subclasses.append(subclass)

    return all_subclasses
