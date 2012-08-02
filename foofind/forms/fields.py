from wtforms import BooleanField, SelectMultipleField
from wtforms.widgets import Input

from cgi import escape
from foofind.utils import u

class HTMLString(unicode):
    def __html__(self):
        return self

class VoidSubmitField(BooleanField):
    class VoidSubmit(Input):
        input_type = 'submit'
        def __call__(self, field, **kwargs):
            kwargs.setdefault('value', field.label.text)
            kwargs.setdefault('id', field.id)
            kwargs.setdefault('type', self.input_type)
            if 'value' not in kwargs: kwargs['value'] = field._value()
            return HTMLString(u'<input %s/>' % html_params(**kwargs))
    widget = VoidSubmit()

def fix_param_name(param):
    if param.startswith("data_"): return "data-%s" % param[5:]
    return param

def html_params(**kwargs):
    params = []
    for k,v in sorted(kwargs.iteritems()):
        if k in ('class_', 'class__', 'for_'): k = k[:-1]
        if v is True: params.append(k)
        else: params.append(u'%s="%s"' % (u(k), escape(u(v), quote=True)))
    return u" ".join(params)

