import os
from dataclasses import dataclass
from jinja2 import Environment


from .mapping import MartMapping
from .context import SourceContext, TargetContext, MappingContext


class SourceObjectExporter:
    src_ctx: SourceContext
    env:     Environment
    
    template_name: str = 'db_table.yaml' #Название шаблона
    
    
    def __init__(self, env, ctx):
        self.env = env
        self.src_ctx = ctx
        
        
    def _get_filled_src(self):
        template = self.env.get_template(self.template_name)
        return template.render(ctx=self.src_ctx)
    
    
    def export(self, path):
        os.makedirs(path, exist_ok=True)
        output = self._get_filled_src()
        with open(f"{path}{self.src_ctx.name}.yaml", "w", encoding="utf-8") as f:
            f.write(output)
        
    
    
class TargetObjectExporter:
    tgt_ctx: TargetContext
    env:     Environment

    template_name_yaml: str = 'mart.yaml'    #Название шаблона yaml
    template_name_sql:  str = 'mart_ddl.sql' #Название шаблона DDL sql 
    template_name_json: str = 'ceh_res.json' #Название шаблона ресурса CEH
    

    def __init__(self, env, ctx):
        self.env = env
        self.tgt_ctx = ctx
        
    
    def _get_filled_tgt(self):
        template = self.env.get_template(self.template_name_yaml)
        return template.render(ctx=self.tgt_ctx)
    
    
    def _get_filled_tgt_sql(self):
        template = self.env.get_template(self.template_name_sql)
        return template.render(ctx=self.tgt_ctx)
    
    
    def _get_filled_tgt_ceh_res(self):
        template = self.env.get_template(self.template_name_json)
        return template.render(ctx=self.tgt_ctx)
    
    
    def export_yaml(self, path):
        os.makedirs(path, exist_ok=True)
        output = self._get_filled_tgt()
        with open(f"{path}{self.tgt_ctx.name}.yaml", "w", encoding="utf-8") as f:
            f.write(output)
            

    def export_sql(self, path):
        os.makedirs(path, exist_ok=True)
        output = self._get_filled_tgt_sql()
        with open(f"{path}{self.tgt_ctx.name}.sql", "w", encoding="utf-8") as f:
            f.write(output)
            
            
    def export_ceh_resource(self, path):
        os.makedirs(path, exist_ok=True)
        output = self._get_filled_tgt_ceh_res()
        with open(f"{path}\\ceh.rdv.{self.tgt_ctx.name}.json", "w", encoding="utf-8") as f:
            f.write(output)
            
    
class MappingObjectExporter:
    map_ctx: MappingContext
    env:     Environment
    wf_file: str
    cf_file: str

    template_wf_name: str = 'wf.yaml' #Название шаблона WF
    template_cf_name: str = 'cf.yaml' #Название шаблона CF
    template_py_name: str = 'wf.py'   #Название PY файла рабочего потока
    
    
    def __init__(self, env, ctx):
        self.env = env
        self.map_ctx = ctx
        self.wf_file = f"wf_{self.map_ctx.src_cd}_{self.map_ctx.source_system.lower()}_rdv_{self.map_ctx.tgt_name[5:]}".lower()
        self.cf_file = f"cf_{self.map_ctx.src_cd}_{self.map_ctx.source_system.lower()}_rdv_{self.map_ctx.tgt_name[5:]}".lower()
    

    def _get_filled_wf_mapping(self):
        template = self.env.get_template(self.template_wf_name)
        return template.render(
            ctx=self.map_ctx,
            wf_file=self.wf_file
        )
    
    
    def _get_filled_cf_mapping(self):
        template = self.env.get_template(self.template_cf_name)
        return template.render(
            ctx=self.map_ctx,
            wf_file=self.wf_file,
            cf_file=self.cf_file
        )
    
    
    def export_wf(self, path):
        os.makedirs(path, exist_ok=True)
        output = self._get_filled_wf_mapping()
        with open(f"{path}{self.wf_file}.yaml".lower(), "w", encoding="utf-8") as f:
            f.write(output)
            
    
    def export_cf(self, path):
        os.makedirs(path, exist_ok=True)
        output = self._get_filled_cf_mapping()
        with open(f"{path}{self.cf_file}.yaml".lower(), "w", encoding="utf-8") as f:
            f.write(output)
            
    
    def export_py(self, path):
        os.makedirs(path, exist_ok=True)
        output = ''
        with open('./templates/wf.py', "r", encoding="utf-8") as f:
            output = f.read()
        with open(f"{path}{self.wf_file}.py", "w", encoding="utf-8") as f:
            f.write(output)    
        


class MartPackExporter:
    exp_obj: MartMapping
    path:    str

    _src_exporter:     SourceObjectExporter
    _tgt_exporter:     TargetObjectExporter
    _mapping_exporter: MappingObjectExporter
    
    def __init__(self, exp_obj, path, env):
        self.exp_obj = exp_obj
        self.path = path
        
        self._src_exporter = SourceObjectExporter(env, self.exp_obj.src_ctx)
        self._tgt_exporter = TargetObjectExporter(env, self.exp_obj.tgt_ctx)
        self._mapping_exporter = MappingObjectExporter(env, self.exp_obj.mapping_ctx)
        
        
    def load(self):
        self._src_export()
        self._tgt_export()
        self._mapping_export()
        
        
    def _src_export(self):
        self._src_exporter.export("{path}\\src_rdv\\schema\\db_tables\\".format(path=self.path))
        
        
    def _tgt_export(self):
        self._tgt_exporter.export_yaml("{path}\\src_rdv\\schema\\ceh\\rdv\\".format(path=self.path))
        self._tgt_exporter.export_sql("{path}\\ddl_sql\\".format(path=self.path))
        self._tgt_exporter.export_ceh_resource("{path}\\_resources\\ceh\\rdv\\".format(path=self.path))

        
    def _mapping_export(self):
        self._mapping_exporter.export_wf("{path}\\src_rdv\\schema\\work_flows\\".format(path=self.path))
        self._mapping_exporter.export_cf("{path}\\src_rdv\\flow_dumps\\".format(path=self.path))
        self._mapping_exporter.export_py("{path}\\src_rdv\\dags\\".format(path=self.path))