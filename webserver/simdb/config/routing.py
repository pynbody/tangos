"""Routes configuration

The more specific and detailed routes should be defined first so they
may take precedent over the more generic routes. For more information
refer to the routes manual at http://routes.groovie.org/docs/
"""
from routes import Mapper

def make_map(config):
    """Create, configure and return the routes Mapper"""
    map = Mapper(directory=config['pylons.paths']['controllers'],
                 always_scan=config['debug'])
    map.minimization = False
    map.explicit = False

    # The ErrorController route (handles 404/500 error pages); it should
    # likely stay at the top, ensuring it can always be resolved
    map.connect('/error/{action}', controller='error')
    map.connect('/error/{action}/{id}', controller='error')




    # CUSTOM ROUTES HERE

    map.connect('/', controller='sims', action='index')
    map.connect('/sims',controller='sims',action='index')
    map.connect('/creator',controller='creator',action='index')

    map.connect('/creator/{action}/{id}',controller='creator')

    #map.connect('/plot/{action}/{timestep_id}/{did1}/{did2}',controller='plot')
    map.connect('/plot/{action}',controller='plot')
    map.connect('/plot/{action}/{id}',controller='plot')
    map.connect('/plot/{action}/{id}/{rel}',controller='plot')


    map.connect('/{action}/{id}',controller='sims')

    map.connect('/showhalo/{id}/{rel}/{num}',controller='sims',action='showhalo')
    map.connect('/showhalo/{id}/{rel}',controller='sims',action='showhalo')
    map.connect('/mergertree/{id}',controller='sims',action='mergertree')
    map.connect('/mergertree/{id}/{rel}/{num}',controller='sims',action='mergertree')

    return map
