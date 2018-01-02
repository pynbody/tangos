from pyramid.view import notfound_view_config


@notfound_view_config(renderer='templates/404.jinja2')
def notfound_view(request):
    request.response.status = 404
    return {}

def includeme(config):
    config.add_static_view('static', 'static', cache_max_age=3600)

    config.add_route('autocomplete_words', '/autocomplete_words.json')
    config.add_route('simulation_list', '/')
    config.add_route('simulation_view', '/{simid}')
    config.add_route('timestep_view', '/{simid}/{timestepid}')
    config.add_route('halo_view', '/{simid}/{timestepid}/{halonumber}')
    config.add_route('halo_later', '/{simid}/{timestepid}/{halonumber}/later/{n}')
    config.add_route('halo_earlier', '/{simid}/{timestepid}/{halonumber}/earlier/{n}')
    config.add_route('halo_in', '/{simid}/{timestepid}/{halonumber}/in/{n}')
    config.add_route('calculate_all', '/{simid}/{timestepid}/gather/{nameid}.json')
    config.add_route('get_property', '/{simid}/{timestepid}/{halonumber}/{nameid}.json')
    config.add_route('gathered_plot', '/{simid}/{timestepid}/{nameid1}/vs/{nameid2}.png')
    config.add_route('gathered_csv', '/{simid}/{timestepid}/{nameid1}/vs/{nameid2}.csv')
    config.add_route('cascade_plot', '/{simid}/{timestepid}/{halonumber}/{nameid1}/vs/{nameid2}.png')
    config.add_route('cascade_csv', '/{simid}/{timestepid}/{halonumber}/{nameid1}/vs/{nameid2}.csv')
    config.add_route('array_plot', '/{simid}/{timestepid}/{halonumber}/{nameid}.png')
    config.add_route('array_csv', '/{simid}/{timestepid}/{halonumber}/{nameid}.csv')
    config.add_route('merger_tree', '/{simid}/{timestepid}/{halonumber}/merger/tree.json')
