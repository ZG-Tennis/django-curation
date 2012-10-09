from collections import Mapping
import textwrap

from django.core import exceptions, validators
from django.db import models
from django.db.models.fields import FieldDoesNotExist
from django.db.models.fields.related import ForeignKey
from django.db.models.fields.related import ReverseSingleRelatedObjectDescriptor
from django import forms
from django.utils.encoding import force_unicode, smart_unicode
from django.utils.text import capfirst

from .generic import GenericForeignKey
from .models import ContentType
from .widgets import SourceSelect

def valid_choice(choices, value):
    """
    Loops through a set of choices, determining whether or not the value 
    provided is going to 

    @param choices: Set of choices to validate against.
    @type  choices: (Key, Value) or [(Key1, Value1), ...]
    
    @param value: Value to check
    @type  value: unicode

    @return: Whether or not the value is valid
    @rtype: Bool
    """
    
    for k, v in self.choices:
        if not isinstance(v, (list, tuple)):
            v = [(k, v)]

        # This is an optgroup, so look inside the group for options
        for k2, _v2 in v:
            if isinstance(k2, Mapping) and "value" in k2:
                k2 = k2['value']

            if value == smart_unicode(k2):
                return True

    return False

class CuratedRelatedField(object):
    """
    A ForeignKey that gets a list of the __dict__ keys and field names of the
    related model on load. It saves this list to the '_proxy_attrs' attribute
    of its parent model _meta attribute.
    """

    def contribute_to_class(self, cls, name):
        sup = super(CuratedRelatedField, self)
        if hasattr(sup, 'contribute_to_class'):
            sup.contribute_to_class(cls, name)

        # Throw a TypeError if there is more than one CuratedForeignKey in
        # the model
        if hasattr(cls._meta, '_curated_proxy_field_name'):
            proxy_field = getattr(cls._meta, '_curated_proxy_field_name')
            raise TypeError('Model %r has more than one CuratedForeignKey: '
                            '%r and %r' % (cls.__name__, proxy_field, name))
        setattr(cls._meta, '_curated_proxy_field_name', name)
        setattr(cls._meta, '_curated_field_is_generic',
            getattr(self, 'ct_field', None) is not None)

    ignore_props = frozenset(
        ('DoesNotExist', 'MultipleObjectsReturned', '__doc__', '_meta', 
         '__module__', '_base_manager', '_default_manager', 'objects')
    )

    def get_proxy_attrs(self, cls):
        """
        Returns the set of fields to allow proxying over from a provided
        class.

        @return: Set of fields to proxy
        @rtype: set("field1", "field2", ...)
        """
        field_names = set(f.name for f in cls._meta.fields)
        return field_names.union(set(cls.__dict__) - self.ignore_props)

    def contribute_to_instance(self, instance, related_cls):
        """
        Because CuratedGenericForeignKey are subclasses of GenericForeignKey
        and so will potentially have different related models across
        instances, instance._meta._proxy_attrs cannot be used as that points
        to ModelClass._meta and so if _proxy_attrs was changed when the
        ContentType for a particular instance changed it would change for all
        instances.

        Note that `contribute_to_instance()`, unlike `contribute_to_class()`
        and `contribute_to_related_class()` is not a standard django field
        method.
        """
        current_proxy_model = instance.__dict__.get('_proxy_model', None)
        if current_proxy_model is related_cls:
            return
        setattr(instance, '_proxy_attrs', self.get_proxy_attrs(related_cls))
        setattr(instance, '_proxy_model', related_cls)

    def contribute_to_related_class(self, cls, related):
        """
        A django built-in that adds attributes to the class a RelatedField
        points to.

        In this case we're adding '_proxy_attrs' to the _meta attribute of the
        ForeignKey's parent model, not the related model. The reason we're not
        using `contribute_to_class` is that we need the related class to be
        instantiated to obtain its field names, and the related class may not
        be loaded yet when `contribute_to_class` is called (for instance, if
        it is lazy loaded, when the ForeignKey field is defined using a string
        for the model).
        """
        sup = super(CuratedRelatedField, self)
        if hasattr(sup, 'contribute_to_related_class'):
            sup.contribute_to_related_class(cls, related)

        setattr(related.model._meta, '_proxy_attrs', self.get_proxy_attrs(cls))

class CuratedForeignKey(CuratedRelatedField, ForeignKey):
    pass


class CuratedGenericForeignKey(CuratedRelatedField, GenericForeignKey):
    pass


class ContentTypeIdChoices(object):
    """
    Iterable used for ContentTypeSourceField's `choices` keyword argument
    """

    ct_choices = None

    def __init__(self, ct_choices):
        self.ct_choices = ct_choices

    def __iter__(self):
        for ct_value, label, source_value in self.ct_choices:
            yield ct_value, label


class SourceChoices(ContentTypeIdChoices):
    """
    Iterable used for the `_choices` attribute of the model field whose name
    was passed in the `source_field` keyword argument to ContentTypeSourceField
    """

    def __iter__(self):
        for ct_value, label, source_value in self.ct_choices:
            yield (source_value, label)


class ContentTypeSourceChoices(object):

    # Sentinel value if a given choice in ct_choices is a 2-tuple and so does
    # not have a source_value
    SOURCE_UNDEFINED = object()
    _cache = None

    error_msgs = {
        'num_items': textwrap.dedent(u"""
            All tuple items in {field_cls}.ct_choices must have two items
            (relation, label,) or three items (relation, label, source_value,)
            """).strip()
    }

    def __init__(self, ct_choices, field):
        self.ct_choices = ct_choices
        self.field = field
        self.ct_lookup = {}
        self.source_value_lookup = {}
        self.ct_ids = set()
        self.source_values = set()
        self.error_msgs['num_items'] = self.error_msgs['num_items'].format(
            field_cls = self.field.__class__.__name__
        )

    def lookup_source_value(self, ct_id):
        """
        Look up the source_value associated with content_type_id=`ct_id`
        """
        if ct_id is None:
            return u""

        if ct_id in self.ct_lookup:
            return self.ct_lookup[ct_id][0]

        if self._cache is None:
            list(self) 
            return self.lookup_source_value(ct_id)

        errors = {}
        errors[self.field.name] = (
            u"Field %(field_name)s on %(app_label)s.%(model_name)s "
            u"does not have a ct_choice item with "
            u"ContentType.id=%(ct_id)d") % {
                'field_name': self.field.source_field_name,
                'app_label': self.field.model._meta.app_label,
                'model_name': self.field.model._meta.object_name,
                'ct_id': ct_id,}
        raise exceptions.ValidationError(errors)

    def lookup_content_type(self, source_value):
        """
        Look up the content_type_id associated with the source value `source_value`
        """
        if source_value is None or force_unicode(source_value) is u"":
            return None

        if source_value in self.source_value_lookup:
            return self.source_value_lookup[source_value][0]

        if self._cache is None:
            list(self)
            return self.lookup_content_type(source_value)

        errors = {}
        errors[self.field.source_field_name] = (
            u"Field %(field_name)s on %(app_label)s.%(model_name)s "
            u"does not have a source_value item with "
            u"source_value=%(source_value)r ") % {
                'field_name': self.field.name,
                'app_label': self.field.model._meta.app_label,
                'model_name': self.field.model._meta.object_name,
                'source_value': source_value,}
        raise exceptions.ValidationError(errors)

    def _process_choices(self):
        model_cls = getattr(self.field, 'model', None)
        for ct_choice in self.ct_choices:

            # If ct_choice is a 3-tuple, get the third item as the source_value
            # Otherwise, it's undefined
            if len(ct_choice) in (2,3):
                relation, label, source_value = (ct_choice + (self.SOURCE_UNDEFINED,))[:3]
            else:
                raise exceptions.ImproperlyConfigured(self.error_msgs['num_items'])

            # Check that the length of this ct_choice item is consistent with
            # previous items
            try:
                if source_val_undefined != self.source_val_undefined:
                    raise exceptions.ImproperlyConfigured(self.error_msgs['num_items'])
            except AttributeError:
                self.source_val_undefined = source_val_undefined

            # We use a dict for the option value so we can add extra attributes
            ct_value = {'class': u'curated-content-type-option'}
            ct_id = None

            # Parse `relation` (the first item in the ct_choice tuple) into
            # app_label and model_name (or field_name, if 'self.something')
            try:
                app_label, model_name = relation.split(".")
            except ValueError:
                # If we can't unpack the tuple, assume a model in current app
                app_label = model_cls._meta.app_label
                model_name = relation
            except AttributeError:
                # If it doesn't have a split it's actually a model class
                app_label = relation._meta.app_label
                model_name = relation._meta.object_name
            else:
                if app_label == 'self' and model_cls:
                    field_name = model_name
                    self.check_field_exists(field_name)
                    # We access this value after render with javascript
                    ct_value['data-field-name'] = field_name
                    ct_value['class'] += u' curated-content-type-ptr'
                    try:
                        ct_id = ContentType.objects.get_for_model(model_cls).pk
                    except model_cls.DoesNotExist:
                        pass

            # If the relation isn't of the form 'self.field_name', grab the
            # content_type_id for the app_label and model_name
            if app_label != 'self':
                ct_model = models.get_model(app_label, model_name, False)
                ct_id = ContentType.objects.get_for_model(ct_model).pk

            ct_value['value'] = ct_id

            if source_value is not self.SOURCE_UNDEFINED:
                self.ct_lookup[ct_id] = (source_value, label)
                self.source_value_lookup[source_value] = (ct_id, label)
                yield ct_value, label, source_value
            else:
                yield ct_value, label

    def __iter__(self):
        try:
            return iter(self._cache)
        except AttributeError:
            self._cache = list(self._process_choices())
            return iter(self._cache)

    def check_field_exists(self, field_name):
        """
        Register the association between this field and a field named in a
        ct_choices item with 'self.field_name'.
        """
        opts = self.field.model._meta
        fields = opts.local_fields + opts.local_many_to_many + opts.virtual_fields
        try:
            next(f for f in fields if f.name == field_name)
        except StopIteration:
            raise FieldDoesNotExist("%s has no field named '%s'" % (
                opts.object_name, field_name))


class ContentTypeSourceDescriptor(ReverseSingleRelatedObjectDescriptor):
    """
    The descriptor for ContentTypeSourceField (the ForeignKey to ContentType)

    Also provides some magic in __set__() that sets the associated source
    field, if provided in the source_field kwarg of ContentTypeSourceField's
    __init__()
    """

    def __init__(self, field_with_rel):
        self.field = field_with_rel

    def __get__(self, instance, instance_type=None):
        return super(ContentTypeSourceDescriptor, self).__get__(instance, instance_type)

    def __set__(self, instance, value):
        if isinstance(value, basestring) and value.isdigit():
            value = ContentType.objects.get_for_id(int(value))

        super(ContentTypeSourceDescriptor, self).__set__(instance, value)

        ct_id = getattr(instance, self.field.attname)

        source_field = getattr(self.field, 'source_field', None)
        if source_field is not None:
            if ct_id is None:
                source_val = None
            else:
                # Lookup the source_value that corresponds to this content
                # type id
                source_val = self.field.ct_choices.lookup_source_value(ct_id)

            # Check if the field already matches to avoid infinite loop
            curr_source_val = getattr(instance, source_field.name, None)
            if source_val != curr_source_val:
                setattr(instance, source_field.name, source_val)


class ContentTypeIdDescriptor(object):
    """
    A descriptor for the `attname` (e.g. content_type_id) of
    ContentTypeSourceField.

    One of the shortfalls of Django's ForeignKey magic is that updates to
    the Field.attname attribute on a model instance (e.g.
    instance.content_type_id for Field.name = 'content_type') does not also
    update the value pointed at by Field.name (instance.content_type). This
    descriptor fixes this problem for ContentTypeSourceField.
    """

    def __init__(self, ct_descriptor):
        # Use __dict__ to allow access to the descriptor object without
        # triggering the __get__() method
        self.__dict__['ct_descriptor'] = ct_descriptor
        self.field = ct_descriptor.field

    def __get__(self, instance, instance_type=None):
        return instance.__dict__.get(self.field.attname)

    def __set__(self, instance, value):
        # Check current value to prevent infinite loop between this descriptor
        # and ContentTypeSourceDescriptor
        try:
            if value == instance.__dict__[self.field.attname]: return
        except KeyError:
            pass

        instance.__dict__[self.field.attname] = value
        instance._ctid_call_count = getattr(instance, '_ctid_call_count', 0) + 1

        # Dumb that python now distinguishes between int/longs
        if isinstance(value, (int, long)):
            value = ContentType.objects.get_for_id(value)
        self.__dict__['ct_descriptor'].__set__(instance, value)

class SourceFieldDescriptor(object):
    """
    A descriptor for the field given in the source_field kwarg to the
    ContentTypeSourceField. The purpose of the descriptor is to keep the
    content_type ForeignKey field and this field synced when this field
    gets assigned a value.
    """

    field = None
    ct_field = None

    def __init__(self, ct_field):
        self.ct_field = ct_field
        self.field = ct_field.source_field

    def __get__(self, instance, instance_type=None):
        if hasattr(self.field, '__get__'):
            self.value = self.field.__get__(instance)
        else:
            self.value = None

        return self.value

    def __set__(self, instance, value):
        if hasattr(self.field, '__set__'):
            self.field.__set__(instance, value)
            # Presumably the descriptor set the attname in the dict
            self.value = instance.__dict__.get(self.field.attname)
        else:
            self.value = instance.__dict__[self.field.attname] = value

        # Did we save anything?  If not, carry on
        if self.ct_field.attname not in instance.__dict__:
            return

        # Set the associated content_type_id for this source value
        ct_id = self.ct_field.ct_choices.lookup_content_type(value)

        # Check the current value to avoid recursive calls
        if ct_id != instance.__dict__.get(self.ct_field.attname):
            setattr(instance, self.ct_field.attname, ct_id)


class ContentTypeChoiceField(forms.TypedChoiceField):
    """
    Formfield for ContentTypeSourceField

    Overrides the widget to allow adding media files and setting classes and
    data-* attributes on <select> and <option> elements.
    """

    def __init__(self, *args, **kwargs):
        field = kwargs.pop('field', None)
        kwargs['widget'] = SourceSelect(attrs={
            'class': 'curated-content-type-select',
            'data-field-name': field.name,
            'data-fk-field-name': field.fk_field,})
        super(ContentTypeChoiceField, self).__init__(*args, **kwargs)

    def valid_value(self, value):
        """
        Check to see if the provided value is a valid choice

        Since we have store the choices values as dictionaries, we need
        to override this method to prevent a ValidationError
        """
        return valid_choice(self.choices, value)

class ContentTypeSourceField(models.ForeignKey):
    """
    A ForeignKey field to the django.contrib.contenttype.models.ContentType
    model (so it does not take the usual positional `to` argument of
    ForeignKey).

    Takes two optional keyword arguments:

    source_field: The name of a field on the model that should be synced
                  with the third values of the ct_choices tuple items. The
                  typical use-case for this is saving a slug for ContentTypes
                  to avoid a JOIN lookup on the django_content_type table in
                  queries.
    ct_choices:   A tuple of 2- or 3-tuples for the ContentType containing
                  (relation, label,[ source_value,]). If source_field has not
                  been passed then ct_choices should be a tuple of 2-tuples.
                  - `relation` is either a model class, the name of another
                    model in the current app, or, if the model is defined in
                    another application, the model specified with the full
                    application label. It also accepts the string
                    'self.fieldname', which toggles the visibility of
                    `fieldname` in the current model's form in the admin based
                    on whether `label` is selected from the content_type
                    dropdown.
                 - `label` is the label that will appear in any admin widgets
                 - `source_value` is the value that will be assigned to the
                    field in `source_field`, if it has been provided. It is
                    better to leave the field referred to be `source_field`
                    out of any ModelAdmins because, unlike the content_type
                    field, it does not support the related-lookup popup.
    """

    ct_choices = None
    fk_field = None
    source_field_name = None
    source_field = None

    def __init__(self, *args, **kwargs):
        ct_choices = kwargs.pop('ct_choices', None)
        if ct_choices is not None:
            self.ct_choices = ContentTypeSourceChoices(ct_choices, self)
            kwargs['choices'] = ContentTypeIdChoices(self.ct_choices)

        self.source_field_name = kwargs.pop('source_field', None)
        super(ContentTypeSourceField, self).__init__(ContentType, *args, **kwargs)

    def contribute_to_class(self, cls, name):
        super(models.ForeignKey, self).contribute_to_class(cls, name)

        content_type_descriptor = ContentTypeSourceDescriptor(self)
        setattr(cls, self.name, content_type_descriptor)
        setattr(cls, self.attname,
                ContentTypeIdDescriptor(content_type_descriptor))

        if isinstance(self.rel.to, basestring):
            target = self.rel.to
        else:
            target = self.rel.to._meta.db_table

        cls._meta.duplicate_targets[self.column] = (target, "o2m")

        # Get source field, if the field name was passed in init, and set its
        # choices
        if self.source_field_name is not None:
            self.source_field = self.model._meta.get_field(self.source_field_name)
            self.source_field._choices = SourceChoices(self.ct_choices)
            # Add / Replace descriptor for the source field that auto-updates
            # the content-type field
            setattr(cls, self.source_field_name, SourceFieldDescriptor(self))

    def validate(self, value, model_instance):
        """
        Validates value and throws ValidationError. Subclasses should override
        this to provide validation logic.

        Since we have store the choices values as dictionaries, we need to
        override this method to prevent a ValidationError
        """
        if not self.editable:
            # Skip validation for non-editable fields.
            return
        if self._choices and value:
            if valid_choice(self.choices, value):
                return

            raise exceptions.ValidationError(
                self.error_messages['invalid_choice'] % value)

        if value is None and not self.null:
            raise exceptions.ValidationError(self.error_messages['null'])

        if not self.blank and value in validators.EMPTY_VALUES:
            raise exceptions.ValidationError(self.error_messages['blank'])

    def formfield(self, **kwargs):
        """
        Returns a django.forms.Field instance for this database Field.

        Rather annoyingly, the parent method does not allow you to override
        the form_class kwarg if self.choices is not None. You have to copy
        and paste everything.
        """
        if not self.choices:
            return super(ContentTypeSourceField, self).formfield(**kwargs)

        defaults = {
            'required': not self.blank,
            'label': capfirst(self.verbose_name),
            'help_text': self.help_text,
        }
        # Many of the subclass-specific formfield arguments (min_value,
        # max_value) don't apply for choice fields, so be sure to only pass
        # the values that TypedChoiceField will understand.
        good_kwargs = set('coerce', 'empty_value', 'choices', 'required',
                          'widget', 'label', 'initial', 'help_text',
                          'error_messages', 'show_hidden_initial')
        for k in set(kwargs) - good_kwargs:
            del kwargs[k]

        defaults.update(kwargs)

        if self.has_default():
            if callable(self.default):
                defaults['initial'] = self.default
                defaults['show_hidden_initial'] = True
            else:
                defaults['initial'] = self.get_default()

        include_blank = self.blank or not (self.has_default() or 'initial' in kwargs)
        defaults['choices'] = self.get_choices(include_blank=include_blank)
        defaults['coerce'] = self.to_python
        if self.null:
            defaults['empty_value'] = None

        defaults['field'] = self

        # All of the above is copy-pasted just so we can set this
        form_class = ContentTypeChoiceField

        return form_class(**defaults)

try:
    from south.modelsinspector import add_introspection_rules
except ImportError:
    pass
else:
    add_introspection_rules([], ["^curation\.fields\.CuratedForeignKey"])
    add_introspection_rules([], ["^curation\.fields\.CuratedGenericForeignKey"])
    add_introspection_rules([], ["^curation\.fields\.ContentTypeSourceField"])
