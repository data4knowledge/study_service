from typing import List
from .alias_code import AliasCode
from .node import *
from .biomedical_concept_property import BiomedicalConceptProperty

class BiomedicalConcept(NodeNameLabel):
  synonyms: List[str] = []
  reference: str
  properties: List[BiomedicalConceptProperty] = []
  code: AliasCode
