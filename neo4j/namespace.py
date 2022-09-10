from py2neo.ogm import Model, Property

class Namespace(Model):
  uuid = Property()
  uri = Property()
