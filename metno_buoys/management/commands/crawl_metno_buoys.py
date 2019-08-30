from thredds_crawler.crawl import Crawl

from django.core.management.base import BaseCommand, CommandError

from geospaas.utils.utils import validate_uri

from metno_buoys.models import MetBuoy

def crawl(url, **options):
    validate_uri(url)

    skips = Crawl.SKIPS + ['.*ncml']
    c = Crawl(url, skip=skips, debug=True)
    added = 0
    for ds in c.datasets:
        url = [s.get('url') for s in ds.services if
                s.get('service').lower()=='opendap'][0]
        metno_obs_stat, cr = MetBuoy.objects.get_or_create(url)
        if cr:
            added += 1
            print('Added %s, no. %d/%d'%(url, added, len(c.datasets)))
    return added

class Command(BaseCommand):
    args = '<url> <select>'
    help = """
        Add metno observation station metadata to the archive. 
        
        Args:
            <url>: the url to the thredds server

        """
    def add_arguments(self, parser):
        parser.add_argument('url', nargs='*', type=str)

    def handle(self, *args, **options):
        if not len(options['url'])==1:
            raise IOError('Please provide a url to the data')
        url = options.pop('url')[0]
        added = crawl(url, **options)
        self.stdout.write(
            'Successfully added metadata of %s metno observation station datasets' %added)


