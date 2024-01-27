from typing import List, Literal
from .base_node import *
from .alias_code import AliasCode
from .biomedical_concept_property import BiomedicalConceptProperty

class BiomedicalConcept(NodeNameLabel):
  synonyms: List[str] = []
  reference: str
  properties: List[BiomedicalConceptProperty] = []
  code: AliasCode
  instanceType: Literal['BiomedicalConcept']
