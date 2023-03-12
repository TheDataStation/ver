# Start the web application of DoD

##  Preparation

1. DoD web appication is compatible with elasticsearch 6.1.0. ES 6.0.0 cannot work properly because it does not support `skip_duplicates` that is neccessory to similarity search.

2. There are minor bugs in app.py. After fixing these bugs, `flask run` can run properly

   Bug 1

   ```python
   app.py
   view_generator = iter(dod.virtual_schema_iterative_search(list_attributes, list_samples))
   # change to
   view_generator = iter(dod.virtual_schema_iterative_search(list_attributes, list_samples,{}))
   ```

   Bug 2

   delete all `global dod` variable excepet the first `global dod` declaration in the app.py

## Run Web Application

1. Start flask server

   ```bash
   $ vim /aurum-datadiscovery/server_config.py
   # edit path_model to your own model path
   ```

   ```bash
   $ cd /aurum-datadiscovery/server-api
   $ flask run
   ```

2. Start the web ui

   ```bash
   $ cd /aurum-datadiscovery/dod/ui
   $ npm start
   ```

   if `npm start` fails, delete `node_modules` in the `ui`, and run `npm run build` to reinstall the `node_modules`. Then run `npm start`  again.