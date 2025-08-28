import os
import uuid

class TimeSeriesChannel:
    def __init__(self, index, name, rate, start, end, type = 'CONTINUOUS', unit = 'uV', group='default', last_annotation=0, properties=[], id=None):
        assert type.upper() in ['CONTINUOUS', 'UNIT'], "Type must be CONTINUOUS or UNIT"

        # metadata for intra-processor tracking
        self.index    = index

        self.id       = id
        self.name     = name.strip()
        self.rate     = rate

        self.start    = int(start)
        self.end      = int(end)

        self.unit     = unit.strip()
        self.type     = type.upper()
        self.group    = group.strip()
        self.last_annotation = last_annotation
        self.properties = properties

    def as_dict(self):
        resp = {
            'name':  self.name,
            'start': self.start,
            'end':   self.end,
            'unit':  self.unit,
            'rate':  self.rate,
            'type':  self.type,
            'group': self.group,
            'lastAnnotation': self.last_annotation,
            'properties': self.properties
        }

        if self.id is not None:
            resp['id'] = self.id

        return resp

    @staticmethod
    def from_dict(channel, properties=None):
        return TimeSeriesChannel(
            name = channel['name'],
            start =  int(channel['start']),
            end =    int(channel['end']),
            unit =   channel['unit'],
            rate =  channel['rate'],
            type =   channel.get('channelType', channel.get('type')),
            group =  channel['group'],
            last_annotation =  int(channel.get('lastAnnotation', 0)),
            properties = channel.get('properties', properties),
            id = channel.get('id'),
            index = -1,
        )

    # custom equality on time series channels for comparing new vs. existing channels
    # equal when name and type are equal and rate is within a small bounded range
    def __eq__(self, other):
        return all([
            self.name.casefold() == other.name.casefold(),
            self.type.casefold() == other.type.casefold(),
            abs(1-(self.rate/other.rate)) < 0.02
        ])
