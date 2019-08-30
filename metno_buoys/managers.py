import warnings
import uuid
import netCDF4
import numpy as np
import pythesint as pti

import datetime
from dateutil.parser import parse

from django.db import models
from django.contrib.gis.geos import GEOSGeometry
from django.contrib.gis.geos import LineString

from geospaas.vocabularies.models import Platform
from geospaas.vocabularies.models import Instrument
from geospaas.vocabularies.models import DataCenter
from geospaas.vocabularies.models import Parameter
from geospaas.vocabularies.models import ISOTopicCategory
from geospaas.catalog.models import GeographicLocation
from geospaas.catalog.models import DatasetURI, Source, Dataset, DatasetParameter

# test url
# uri = https://thredds.met.no/thredds/dodsC/obs/kystverketbuoy/2019/07/201907_Kystverket-Smartbuoy-Fauskane_Weather-Station-GillWindSensor.nc
class MetBuoyManager(models.Manager):

    def get_or_create(self, uri, *args, **kwargs):
        ''' Create dataset and corresponding metadata

        Parameters:
        ----------
            uri : str
                  URI to file or stream openable by netCDF4.Dataset
        Returns:
        -------
            dataset and flag
        '''
        # check if dataset already exists
        uris = DatasetURI.objects.filter(uri=uri)
        if len(uris) > 0:
            print(uri + ': Already ingested')
            return uris[0].dataset, False

        # set source
        platform = pti.get_gcmd_platform('buoys')
        instrument = pti.get_gcmd_instrument('in situ/laboratory instruments')

        pp = Platform.objects.get(
                category=platform['Category'],
                series_entity=platform['Series_Entity'],
                short_name=platform['Short_Name'],
                long_name=platform['Long_Name']
            )
        ii = Instrument.objects.get(
                category = instrument['Category'],
                instrument_class = instrument['Class'],
                type = instrument['Type'],
                subtype = instrument['Subtype'],
                short_name = instrument['Short_Name'],
                long_name = instrument['Long_Name']
            )
        source = Source.objects.get_or_create(
            platform = pp,
            instrument = ii)[0]

        try:
            nc_dataset = netCDF4.Dataset(uri)
        except OSError:
            nc_dataset = netCDF4.Dataset(uri+'#fillmismatch')

        if not 'time_coverage_start' in nc_dataset.ncattrs() or not nc_dataset.time_coverage_start:
            time_coverage_start = parse(nc_dataset.variables['time'].units.split()[2]) + \
                    datetime.timedelta(seconds=nc_dataset.variables['time'][:].data[0])
        else:
            time_coverage_start=parse(nc_dataset.time_coverage_start),

        if not 'time_coverage_end' in nc_dataset.ncattrs() or not nc_dataset.time_coverage_end:
            time_coverage_end = parse(nc_dataset.variables['time'].units.split()[2]) + \
                    datetime.timedelta(seconds=nc_dataset.variables['time'][:].data[-1])
        else:
            time_coverage_end = parse(nc_dataset.time_coverage_end),

        longitude = nc_dataset.variables['longitude'][:].data
        latitude = nc_dataset.variables['latitude'][:].data
        geometry = LineString(np.column_stack((longitude, latitude)))
        geolocation = GeographicLocation.objects.get_or_create(
                            geometry=geometry)[0]

        # in case of a point..
        #location = GEOSGeometry('POINT(%s %s)' % (longitude, latitude))
        #geolocation = GeographicLocation.objects.get_or_create(
        #                    geometry=location)[0]

        entrytitle = nc_dataset.title
        dc = DataCenter.objects.get(short_name='NO/MET')
        iso_category = ISOTopicCategory.objects.get(name='Climatology/Meteorology/Atmosphere')
        if 'summary' in nc_dataset.ncattrs():
            summary = nc_dataset.summary
        else:
            summary = nc_dataset.title

        if 'entry_id' in nc_dataset.ncattrs():
            entry_id = nc_dataset.entry_id
        elif 'id' in nc_dataset.ncattrs():
            entry_id = nc_dataset.id
        elif 'station_name' in nc_dataset.ncattrs():
            id0 = nc_dataset.station_name.replace(' ', '')
            lastds = self.filter(entry_id__contains=id0).last()
            if lastds:
                next_buoy_num = int(lastds.entry_id.replace(id0,'')) + 1
            else:
                next_buoy_num = 1
            entry_id = id0 + str(next_buoy_num)

        ds = Dataset(
                entry_id = entry_id,
                entry_title = entrytitle,
                ISO_topic_category = iso_category,
                data_center = dc,
                summary = summary,
                time_coverage_start = time_coverage_start,
                time_coverage_end = time_coverage_end,
                source = source,
                geographic_location = geolocation)
        ds.save()

        ds_uri = DatasetURI.objects.get_or_create(uri=uri, dataset=ds)[0]

        # Add dataset parameters
        vars = nc_dataset.variables
        time = vars.pop('time')
        lat = vars.pop('latitude')
        lon = vars.pop('longitude')
        id = vars.pop('station_id', '')
        for key in vars.keys():
            if not 'standard_name' in vars[key].ncattrs():
                continue
            try:
                par = Parameter.objects.get(standard_name=vars[key].standard_name)
            except Parameter.DoesNotExist as e:
                warnings.warn('{}: {}'.format(vars[key].standard_name, e.args[0]))
                continue
            dsp = DatasetParameter(dataset=ds, parameter=par)
            dsp.save()

        return ds, True



