

from slimmer import css_slimmer, js_slimmer
from webassets.filter import Filter, register_filter

class JsSlimmer(Filter):
    name = 'js_slimmer'

    def output(self, _in, out, **kwargs):
        out.write(js_slimmer(_in.read()))


class CssSlimmer(Filter):
    name = 'css_slimmer'

    def output(self, _in, out, **kwargs):
        out.write(css_slimmer(_in.read()))
