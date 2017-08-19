def includeme(config):
    config.add_static_view('static', 'static', cache_max_age=3600)
    config.add_route('simulation_list', '/')
    config.add_route('simulation_view', '/{simid}')
    config.add_route('timestep_view', '/{simid}/{timestepid}')
    config.add_route('halo_view', '/{simid}/{timestepid}/{halonumber}')
    config.add_route('halo_later', '/{simid}/{timestepid}/{halonumber}/later/{n}')
    config.add_route('halo_earlier', '/{simid}/{timestepid}/{halonumber}/earlier/{n}')
    config.add_route('gather_property', '/{simid}/{timestepid}/gather/{nameid}.json')
    config.add_route('get_property', '/{simid}/{timestepid}/{halonumber}/{nameid}.json')
    