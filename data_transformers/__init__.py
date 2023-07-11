from typing import Any, Callable, Dict, List, Union
from functools import wraps, partial


class BaseField:
    def __call__(self, row: Dict[str, Any]) -> Any:
        raise NotImplementedError("BaseField is an abstract class")


class Field(BaseField):
    def __init__(
        self,
        name: str,
        default: Any = None,
        filter: Callable[[Any], Any] = None
    ) -> None:
        self.name = name
        self.filter = filter
        self.default = default

    def __call__(self, row: Dict[str, Any]) -> Any:
        data = row.get(self.name, self.default)

        if self.filter and callable(self.filter):
            data = self.filter(data)

        return data


class Transformer:
    @property
    def fields(self) -> Dict[str, Union[BaseField, Callable[[Dict[str, Any]], Any]]]:
        defined_fields = {
            key: value
            for key, value in vars(self.__class__).items()
            if isinstance(value, BaseField)
        }

        dynamic_fields = {
            key[4:]: partial(value, self)
            for key, value in vars(self.__class__).items()
            if key.startswith("get_") and not isinstance(value, BaseField)
        }

        return {**defined_fields, **dynamic_fields}

    @property
    def field_names(self) -> List[str]:
        return list(self.fields.keys())

    def transform_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {k: field(row) for k, field in self.fields.items()}

    def transform(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [self.transform_row(row) for row in rows]


class CombineTransformers:
    def __init__(self, **transformers: Transformer) -> None:
        assert all(isinstance(t, Transformer) for t in transformers.values())

        self.transformers = transformers

    def transform(self, rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        return {
            name: transformer.transform(rows)
            for name, transformer in self.transformers.items()
        }

    def transform_row(self, row: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        return {
            name: transformer.transform_row(row)
            for name, transformer in self.transformers.items()
        }
