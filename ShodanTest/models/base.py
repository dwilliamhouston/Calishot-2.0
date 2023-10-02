from typing import (Any,Dict, List)

from sqlalchemy.orm import DeclarativeBase

class SQLModel(DeclarativeBase):
    """Based class used for model definitions."""

    @classmethod
    def schema(cls) -> str:
        _schema = cls.__mapper__.selectable.schema
        if _schema is None:
            raise ValueError("Cannot identify model schema")
        return _schema
    
    @classmethod
    def table_name(cls) -> str:
        return cls.__tablename__
    
    @classmethod
    def fields(cls) -> List[str]:
        return cls.__mapper__.selectable.c.keys()

    def to_dict(self) -> Dict[str, Any]:
    
        _dict: Dict[str, Any] = dict()
        for key in self.__mapper__.c.keys():
            _dict[key] = getattr(self, key)
        return _dict
    