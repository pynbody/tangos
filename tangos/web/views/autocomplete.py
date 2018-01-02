from pyramid.view import view_config
from ...core import dictionary
from ... import properties, live_calculation
@view_config(route_name='autocomplete_words', renderer='json')
def autocomplete_words(request):
    dictionary_items = list(dictionary.get_lexicon(request.dbsession))
    liveproperties = properties.all_properties(with_particle_data=False)+live_calculation.BuiltinFunction.all()
    liveproperties = list(map(lambda x: x+"(", liveproperties))
    return dictionary_items+liveproperties
