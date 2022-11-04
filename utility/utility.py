def format_name(name):
    name = name.lower()
    name = name.replace(" ", "_")
    return name

def extend_uri(base, ext):
  if base[-1] == '/':
    return "%s%s" % (base, ext)
  else:
    return "%s/%s" % (base, ext)

      