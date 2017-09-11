from __future__ import absolute_import
from pyramid.config import Configurator


def main(global_config, **settings):
    """ This function returns a Pyramid WSGI application.
    """
    config = Configurator(settings=settings)
    config.include('pyramid_jinja2')
    config.include('.models')
    config.include('.routes')
    config.scan()

    from . import crumbs
    config.add_request_method(crumbs.breadcrumbs, 'breadcrumbs', reify=True)


    app = config.make_wsgi_app()

    j_env = config.get_jinja2_environment()
    j_env.globals.update(zip=zip,len=len)

    return app
