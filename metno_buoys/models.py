from django.db import models

from geospaas.catalog.models import Dataset as CatalogDataset

from metno_buoys.managers import MetBuoyManager

class MetBuoy(CatalogDataset):
    class Meta:
        proxy = True
    objects = MetBuoyManager()


