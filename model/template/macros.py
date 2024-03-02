# import os
# import base64
import warnings
from bs4 import BeautifulSoup
from d4kms_generic import application_logger
from model.element.element_manager import ElementManager
from model.base_node import *

class Macros():

  class NO_STUDY_VERSION(Exception):
    pass

  AS_VALUE = "value"
  AS_REFERENCE = "reference"

  def __init__(self, study_version):
    self._study_version = study_version
    self._element_manager = ElementManager(study_version)
    # self.study_design = self.study_version.studyDesigns[0]
    # self.protocol_document_version = self.study.documentedBy.versions[0]
    # self.elements = Elements(parent, self.study)
    # self.m11 = TemplateM11(parent, self.study)
    # self.plain = TemplatePlain(parent, self.study)
    # self.template_map = {'m11': self.m11, 'plain': self.plain}
  
  def resolve(self, text: str, resolution_type=AS_VALUE) -> str:
    soup = self._get_soup(text)
    for ref in soup(['usdm:ref','usdm:macro']):
      try:
        attributes = ref.attrs
        print(f"RESOLVE: T={ref.name}, A={attributes}")
        method = f"_{attributes['id'].lower()}" if ref.name == 'usdm:macro' else f"_ref"
        if self._valid_method(method):            
          getattr(self, method)(attributes, soup, ref, resolution_type)
        else:
          application_logger.error(f"Failed to resolve document macro '{attributes}', invalid method name {method}")
          ref.replace_with('Missing content: invalid method name')
      except Exception as e:
        application_logger.exception(f"Exception raised while attempting to resolve document macro '{attributes}'", e)
        ref.replace_with('Missing content: exception')
    return str(soup)

  def _ref(self, attributes, soup, ref, type) -> None:
    instance = NodeId.find_by_id(attributes['klass'], attributes['id'])
    text = getattr(instance, attributes['attribute'])
    ref.replace_with(text)

  # def _image(self, attributes, soup, ref) -> None:
  #   type = {attributes['type']}
  #   data = self._encode_image(attributes['file'])
  #   if data:
  #     img_tag = soup.new_tag("img")
  #     img_tag.attrs['src'] = f"data:image/{type};base64,{data.decode('ascii')}"
  #     ref.replace_with(img_tag)
  #   else:
  #     self._note({'text': f"Failed to insert image '{attributes['file']}', ignoring!"}, soup, ref)

  def _element(self, attributes, soup, ref, type) -> None:
    assert self._study_version
    name = attributes['name'].lower()
    element = self._element_manager.element(name)
    info = element.reference()
    if type == self.AS_VALUE:
      replacement = getattr(info['result']['instance'], info['result']['attribute'])
    else:
      replacement = self._usdm_reference(self, soup, info['result']['instance'], info['result']['attribute'])
    ref.replace_with(self._get_soup(replacement))

  def _text_section(self, attributes, soup, ref, type) -> None:
    if type == self.AS_VALUE:
      replacement = "<div></div>"
    else:
      replacement = "<div></div>"
    ref.replace_with(self._get_soup(replacement))

  # def _section(self, attributes, soup, ref) -> None:
  #   method = attributes['name'].lower()
  #   template = attributes['template'] if 'template' in attributes else 'plain' 
  #   instance = self._resolve_template(template)
  #   if instance.valid_method(method):
  #     text = getattr(instance, method)()
  #     ref.replace_with(get_soup(text, self.parent))
  #   else:
  #     self.parent._general_error(f"Failed to translate section method name '{method}' in '{attributes}', invalid method")
  #     ref.replace_with('Missing content: invalid method name')       

  # def _note(self, attributes, soup, ref) -> None:
  #   text = f"""
  #     <div class="col-md-8 offset-md-2 text-center">
  #       <p class="usdm-warning">Note:</p>
  #       <p class="usdm-warning">{attributes['text']}</p>
  #     </div>
  #   """
  #   ref.replace_with(get_soup(text, self.parent))

  def _valid_method(self, name):
    return name in ['_element', '_ref', '_text_section']
    #return name in ['_xref', '_image', '_element', '_section', '_note']
  
  # def _resolve_template(self, template) -> object:
  #   try:
  #     return self.template_map[template.lower()]
  #   except:
  #     self.parent._general_error(f"Failed to map template '{template}', using plain template")
  #     return self.plain

  # def _encode_image(self, filename) -> bytes:
  #   try:
  #     with open(os.path.join(self.parent.dir_path, filename), "rb") as image_file:
  #       data = base64.b64encode(image_file.read())
  #     return data
  #   except:
  #     self.parent._general_error(f"Failed to open image file '{filename}', ignored")
  #     return None

  def _usdm_reference(self, soup, instance, attribute):
    ref_tag = soup.new_tag("usdm:ref")
    ref_tag.attrs = {'klass': instance.__class__.__name__, 'id': instance.id, 'attribute': attribute}
    return ref_tag

  def _get_soup(self, text):
    try:
      with warnings.catch_warnings(record=True) as warning_list:
        result =  BeautifulSoup(text, 'html.parser')
      if warning_list:
        for item in warning_list:
          application_logger.warning(f"Warning raised within Soup package, processing '{text}'\nMessage returned '{item.message}'")
      return result
    except Exception as e:
      application_logger.exception(f"Exception raised parsing '{text}'. Ignoring value", e)
      return ""
  