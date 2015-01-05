{application, dork,
 [{description, "DorkDb"},
  {vsn, "0.1.0"},
  {modules, [
    dork,
    dorkdb,
  ]},
  {registered, []},
  {mod, {dork, []}},
  {env, []},
  {applications, [kernel, stdlib]}]}.
