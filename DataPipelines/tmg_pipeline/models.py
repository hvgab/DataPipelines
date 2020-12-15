from django.db import models
from django.db.models.fields import CharField
from django.db.models.fields.related import ForeignKey

# Create your models here.

class TmgServer(models.Model):
    name = models.CharField()
    host = models.CharField()
    dbname = models.CharField(blank=True, null=True)
    username = models.CharField()
    password = models.CharField()

    
class Preset(models.Model):
    pass

class QueryResult(models.Model):
    """All SQL queries are saved"""
    query = models.TextField()
    result = models.JSONField()
    datetime = models.DateTimeField(auto_now=True)


class BasicExport(models.Model):
    server = ForeignKey('TmgServer')
    project = models.CharField()
    projectJobs = models.CharField()
    preset = models.ForeignKey('Preset')
    add_prefix = models.BooleanField(default=False)
    session_state_in_filter = models.CharField()
    dialplan_contactresponse_in_filter = models.CharField()
    drop_cols = models.TextField()
    horizontal_orders = models.BooleanField(default=False)
    