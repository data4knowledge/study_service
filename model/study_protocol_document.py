from typing import List, Literal
from .base_node import NodeNameLabelDesc
from .study_protocol_document_version import StudyProtocolDocumentVersion

class StudyProtocolDocument(NodeNameLabelDesc):
  versions: List[StudyProtocolDocumentVersion] = []
  instanceType: Literal['StudyProtocolDocument']