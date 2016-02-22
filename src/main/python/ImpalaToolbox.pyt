import arcpy
import impala.dbapi
import math
import os
import re
import traceback


class HexGrid:
    def __init__(self, orig_x=0.0, orig_y=0.0, size=100):
        self.orig_x = orig_x
        self.orig_y = orig_y
        self.size = size
        self.h = self.size * math.cos(30.0 * math.pi / 180.0)
        self.v = self.size * 0.5
        self.skip_x = 2.0 * self.h
        self.skip_y = 3.0 * self.v

    def rc2xy(self, r, c):
        ofs = self.h if r % 2 != 0 else 0
        x = c * self.skip_x + ofs + self.orig_x
        y = r * self.skip_y + self.orig_y
        return x, y


class HexCell:
    def __init__(self, size):
        self.xy = []
        for i in range(7):
            angle = math.pi * ((i % 6) + 0.5) / 3.0
            x = size * math.cos(angle)
            y = size * math.sin(angle)
            self.xy.append((x, y))

    def toShape(self, cx, cy):
        return [[cx + x, cy + y] for (x, y) in self.xy]


class BaseTool(object):
    def __init__(self):
        self.RAD = 6378137.0
        self.RAD2 = self.RAD * 0.5
        self.LON = self.RAD * math.pi / 180.0
        self.D2R = math.pi / 180.0

    def lonToX(self, l):
        return l * self.LON

    def latToY(self, l):
        rad = l * self.D2R
        sin = math.sin(rad)
        return self.RAD2 * math.log((1.0 + sin) / (1.0 - sin))

    def deleteFC(self, fc):
        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)

    def getParamString(self, name="in", display_name="Label", value=""):
        param = arcpy.Parameter(
                name=name,
                displayName=display_name,
                direction="Input",
                datatype="String",
                parameterType="Required")
        param.value = value
        return param

    def getParamHost(self, display_name="Host", value="sandbox"):
        return self.getParamString(name="in_host", display_name=display_name, value=value)

    def getParamSize(self, display_name="Hex size in meters", value="100"):
        return self.getParamString(name="in_size", display_name=display_name, value=value)

    def getParamName(self, display_name="Layer name", value="output"):
        return self.getParamString(name="in_name", display_name=display_name, value=value)

    def getParamPath(self, display_name="HDFS path", value="/user/hadoop/output"):
        return self.getParamString(name="in_path", display_name=display_name, value=value)

    def getParamFC(self):
        output_fc = arcpy.Parameter(
                name="output_fc",
                displayName="output_fc",
                direction="Output",
                datatype="Feature Layer",
                parameterType="Derived")
        return output_fc

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return


class Toolbox(object):
    def __init__(self):
        self.label = "Toolbox"
        self.alias = "Impala Toolbox"
        self.tools = [QueryTripsTool, DensityTool, HexTool]


class QueryTripsTool(BaseTool):
    def __init__(self):
        super(QueryTripsTool, self).__init__()
        self.label = "Query Trips"
        self.description = "Tool to query trips table using Impala"
        self.canRunInBackground = False

    def getParameterInfo(self):
        paramWhere = self.getParamString(name="in_where", display_name="Where", value="hour between 7 and 9")
        return [paramWhere, self.getParamName(value="trips79"), self.getParamFC()]

    def execute(self, parameters, messages):
        try:
            name = parameters[1].value

            in_memory = True
            if in_memory:
                ws = "in_memory"
                fc = ws + "/" + name
            else:
                fc = os.path.join(arcpy.env.scratchGDB, name)
                ws = os.path.dirname(fc)
            self.deleteFC(fc)

            sp_ref = arcpy.SpatialReference(102100)
            arcpy.management.CreateFeatureclass(ws, name, "POINT", spatial_reference=sp_ref)
            arcpy.management.AddField(fc, "PICKUP_DT", "TEXT")
            arcpy.management.AddField(fc, "PASS_COUNT", "SHORT")
            arcpy.management.AddField(fc, "TRIP_TIME", "SHORT")
            arcpy.management.AddField(fc, "TRIP_DIST", "FLOAT")
            conn = impala.dbapi.connect(host='quickstart', port=21050)
            rows = conn.cursor()
            rows.set_arraysize(1024 * 10)
            hql = """select
                pickupx,
                pickupy,
                pickupdatetime,
                passengercount,
                triptime,
                tripdist
                from trips
                where {w}""".format(w=parameters[0].value)
            hql = re.sub(r'\s+', ' ', hql)
            rows.execute(hql)
            with arcpy.da.InsertCursor(fc,
                                       ['SHAPE@XY', 'PICKUP_DT', 'PASS_COUNT', 'TRIP_TIME', 'TRIP_DIST']) as cursor:
                for row in rows:
                    cursor.insertRow([(row[0], row[1]), row[2], row[3], row[4], row[5]])
                del row
            del rows
            parameters[2].value = fc
        except:
            arcpy.AddMessage(traceback.format_exc())


class DensityTool(BaseTool):
    def __init__(self):
        super(DensityTool, self).__init__()
        self.label = "Trip Density"
        self.description = "Calculate density of trips"
        self.canRunInBackground = False

    def getParameterInfo(self):
        paramCell = self.getParamSize(value="100")
        paramWhere = self.getParamString(name="in_where", display_name="Where", value="hour between 7 and 9")
        return [paramCell, paramWhere, self.getParamName(value="density79"), self.getParamFC()]

    def execute(self, parameters, messages):
        try:
            cell1 = float(parameters[0].value)
            cell2 = cell1 * 0.5
            where = parameters[1].value
            name = parameters[2].value

            in_memory = True
            if in_memory:
                ws = "in_memory"
                fc = ws + "/" + name
            else:
                fc = os.path.join(arcpy.env.scratchGDB, name)
                ws = os.path.dirname(fc)
            self.deleteFC(fc)

            sp_ref = arcpy.SpatialReference(102100)
            arcpy.management.CreateFeatureclass(ws, name, "POINT", spatial_reference=sp_ref)
            arcpy.management.AddField(fc, "POPULATION", "LONG")
            conn = impala.dbapi.connect(host='quickstart', port=21050)
            rows = conn.cursor()
            hql = """select
                T.C*{c1}+{c2} as X,
                T.R*{c1}+{c2} as Y,
                count(*) AS POPULATION from (
                    select
                    cast(floor(pickupx/{c1}) as int) as C,
                    cast(floor(pickupy/{c1}) as int) as R
                    from trips where {w}) T
                    group by T.R,T.C
            """.format(w=where, c1=cell1, c2=cell2)
            hql = re.sub(r'\s+', ' ', hql)
            rows.execute(hql)
            with arcpy.da.InsertCursor(fc, ['SHAPE@XY', 'POPULATION']) as cursor:
                for row in rows:
                    cursor.insertRow([(row[0], row[1]), row[2]])
                del row
            del rows
            parameters[3].value = fc
        except:
            arcpy.AddMessage(traceback.format_exc())


class HexTool(BaseTool):
    def __init__(self):
        super(HexTool, self).__init__()
        self.label = "Hex Density"
        self.description = "Calculate density based on hex cells"
        self.canRunInBackground = False

    def getParameterInfo(self):
        paramCell = self.getParamSize(value="100")
        paramWhere = self.getParamString(name="in_where", display_name="Where", value="hour between 7 and 9")
        paramName = self.getParamName(value="hex79")
        return [paramCell, paramWhere, paramName, self.getParamFC()]

    def execute(self, parameters, messages):
        try:
            cell = parameters[0].value
            size = float(cell)
            hex_cell = HexCell(size=size)
            hex_grid = HexGrid(size=size)
            where = parameters[1].value
            name = parameters[2].value

            in_memory = True
            if in_memory:
                ws = "in_memory"
                fc = ws + "/" + name
            else:
                fc = os.path.join(arcpy.env.scratchGDB, name)
                ws = os.path.dirname(fc)
            self.deleteFC(fc)

            sp_ref = arcpy.SpatialReference(102100)
            arcpy.management.CreateFeatureclass(ws, name, "POLYGON", spatial_reference=sp_ref)
            arcpy.management.AddField(fc, "POPULATION", "LONG")
            conn = impala.dbapi.connect(host='quickstart', port=21050)
            rows = conn.cursor()
            hql = """select rc{c},count(rc{c})
            from trips
            where {w}
            group by rc{c}
            """.format(c=cell, w=where)
            hql = re.sub(r'\s+', ' ', hql)
            arcpy.AddMessage(hql)
            rows.execute(hql)
            with arcpy.da.InsertCursor(fc, ['SHAPE@', 'POPULATION']) as cursor:
                for row in rows:
                    r, c = row[0].split(":")
                    x, y = hex_grid.rc2xy(float(r), float(c))
                    cursor.insertRow([hex_cell.toShape(x, y), row[1]])
            parameters[3].value = fc
        except:
            arcpy.AddMessage(traceback.format_exc())
