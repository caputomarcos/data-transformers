from datetime import datetime, timedelta
from typing import Any, Dict
import uuid

from data_transformers import CombineTransformers, Transformer, Field


class SampleTransformer(Transformer):
  
    id = Field("id", default=lambda: str(uuid.uuid4()))
    purchase_date = Field("purchase_date")
    description = Field("description", filter=lambda x: x.upper())
    
    def get_warranty_end(self, row: Dict[str, Any]) -> Any:
        purchase_date = datetime.strptime(row.get("purchase_date"), "%Y-%m-%dT%H:%M:%S")
        warranty_end_date = purchase_date + timedelta(days=365 * 3)
        return warranty_end_date.isoformat()

    def get_price(self, row: Dict[str, Any]) -> float:
        commission = row.get("Commission", {})
        percent = commission.get('Percent', 0)
        cust = row.get("cust", 0)
        return percent * cust
      
    def get_latitude(self, row: Dict[str, Any]) -> float:
        position = row.get("Position")
        return position["Latitude"]

    def get_longitude(self, row: Dict[str, Any]) -> float:
        position = row.get("Position")
        return position["Longitude"]


data = [
    {
        "id": "37e991ae-04cb-4aa6-a0cf-1451eb58dadc",
        "name": "car",
        "description": "description",
        "purchase_date": "2023-05-01T00:00:00",
        "Commission": {"Percent": 2.0},
        "cust": 50,
        "AmountIncludingMarkup": True,
        "Position": {"Latitude": 42.1234, "Longitude": 5.5678},
    },
    {
        "name": "laptop",
        "description": "description",
        "purchase_date": "2023-05-01T00:00:00",
        "Commission": {"Percent": 1.3},
        "cust": 104,
        "AmountIncludingMarkup": True,
        "Position": {"Latitude": 52.5, "Longitude": 21.3},
    },
]

transformer = CombineTransformers(
    first=SampleTransformer(),
    second=SampleTransformer(),
)

transformer = SampleTransformer()
print(transformer.transform(data))
