import csv
from io import StringIO

# CSV renderer based on pyramid example
# https://docs.pylonsproject.org/projects/pyramid-cookbook/en/latest/templates/customrenderers.html

class CSVRenderer:
    def __init__(self, info):
        pass

    def __call__(self, value, system):
        """ Returns a plain CSV-encoded string with content-type
        ``text/csv``. The content-type may be overridden by
        setting ``request.response.content_type``."""
        request = system.get('request')
        if request is not None:
            response = request.response
            ct = response.content_type
            if ct == response.default_content_type:
                response.content_type = 'text/csv'
            if 'name' in value:
                response.content_disposition = 'attachment;filename=' + value['name'] + '.csv'

        fout = StringIO()
        writer = csv.writer(fout, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        writer.writerow(value.get('header', []))
        writer.writerows(value.get('rows', []))

        return fout.getvalue()
