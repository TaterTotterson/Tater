from __future__ import annotations
import ast
import json
from pathlib import Path
from types import SimpleNamespace
import time
import unittest

ROOT=Path(__file__).resolve().parents[1]
def functions(filename,names,namespace):
    tree=ast.parse((ROOT/filename).read_text())
    nodes=[node for node in tree.body if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)) and node.name in names]
    exec(compile(ast.fix_missing_locations(ast.Module(body=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0),*nodes],type_ignores=[])),filename,'exec'),namespace)
    return namespace

class CoreWorkspaceBackendTests(unittest.TestCase):
    def setUp(self):
        self.registry=functions('core_registry.py',{'_load_workspace_metadata','_load_webui_tab_config','_coerce_bool'},{'json':json})
    def test_metadata_is_optional_bounded_data(self):
        load=self.registry['_load_workspace_metadata'];self.assertEqual(load(None),{});self.assertEqual(load({'schema':2}),{})
        self.assertEqual(load({'schema':1,'invalid':object()}),{});self.assertEqual(load({'schema':1,'large':'x'*32768}),{})
        raw={'schema':1,'views':{'items':'list'}};copy=load(raw);copy['views']['items']='other';self.assertEqual(raw['views']['items'],'list')
    def test_legacy_and_new_core_definitions_load(self):
        load=self.registry['_load_webui_tab_config'];old=load(SimpleNamespace(CORE_WEBUI_TAB={'label':'Test','requires_running':True}));self.assertEqual(old['workspace'],{})
        new=load(SimpleNamespace(CORE_WEBUI_TAB={'label':'Test'},CORE_WORKSPACE={'schema':1,'default_tab':'items'}));self.assertEqual(new['workspace']['default_tab'],'items')
    def app(self,provider=None):
        runtime=SimpleNamespace(is_running=lambda key:False,_import_module=lambda *a,**k:SimpleNamespace(get_htmlui_tab_data=provider))
        return functions('tateros_app.py',{'_discover_runtime_webui_tabs','_discover_core_webui_tabs','_load_surface_htmlui_tab_payload'},{'core_registry_module':SimpleNamespace(_load_workspace_metadata=self.registry['_load_workspace_metadata']),'redis_client':SimpleNamespace(get=lambda key:'false'),'core_runtime':runtime,'_as_bool_flag':lambda value,default:default if value is None else bool(value),'time':time})
    def test_stopped_cores_remain_in_workspace_directory(self):
        ns=self.app();tabs=ns['_discover_core_webui_tabs']([{'key':'example_core','has_webui_tab_renderer':True,'webui_tab':{'label':'Example','requires_running':True,'workspace':{'schema':1,'icon':'calendar'}}}]);self.assertEqual(len(tabs),1);self.assertFalse(tabs[0]['running']);self.assertEqual(tabs[0]['workspace']['icon'],'calendar')
    def test_presentation_does_not_replace_dynamic_fields_actions_or_provider_ui(self):
        ui={'kind':'settings_manager','item_forms':[{'id':'one','fields':[{'key':'dynamic','options':[{'value':'new'}]}],'save_action':'save_one'}],'workspace':{'schema':1,'default_tab':'dynamic'}}
        raw={'ui':ui,'stats':[{'label':'Saved','value':3}]};ns=self.app(lambda **kwargs:raw)
        payload=ns['_load_surface_htmlui_tab_payload']({'core_key':'example_core','workspace':{'schema':1,'icon':'cores'}})
        self.assertIs(payload['ui']['item_forms'],ui['item_forms']);self.assertEqual(payload['ui']['workspace'],{'schema':1,'icon':'cores','default_tab':'dynamic'});self.assertNotIn('icon',ui['workspace'])

if __name__=='__main__':unittest.main()
