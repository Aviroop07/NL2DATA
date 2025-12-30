import React from 'react';
import { Box, Typography, Paper, Divider, Chip, Stack } from '@mui/material';
import { CheckCircle, RadioButtonUnchecked } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import { formatTimestamp } from '../../utils/helpers';

const StatusTrail: React.FC = () => {
  const { statusTrail } = useAppStore();
  
  if (statusTrail.length === 0) {
    return (
      <Paper 
        elevation={0}
        sx={{ 
          p: 3, 
          textAlign: 'center',
          bgcolor: 'grey.50',
          border: '1px dashed',
          borderColor: 'divider'
        }}
      >
        <Typography variant="body2" color="text.secondary">
          No status updates yet
        </Typography>
      </Paper>
    );
  }
  
  return (
    <Paper 
      elevation={0}
      sx={{ 
        maxHeight: 400, 
        overflow: 'auto',
        border: '1px solid',
        borderColor: 'divider',
        borderRadius: 2
      }}
    >
      <Box sx={{ p: 2 }}>
        {statusTrail.map((tick, index) => (
          <Box key={index}>
            <Box 
              sx={{ 
                display: 'flex', 
                gap: 2,
                py: 1.5,
                position: 'relative',
                pl: 3,
                '&::before': {
                  content: '""',
                  position: 'absolute',
                  left: 7,
                  top: index === 0 ? 20 : 0,
                  bottom: index === statusTrail.length - 1 ? 20 : 0,
                  width: 2,
                  bgcolor: index === statusTrail.length - 1 ? 'primary.main' : 'grey.300',
                  opacity: index === statusTrail.length - 1 ? 1 : 0.5,
                }
              }}
            >
              <Box sx={{ position: 'absolute', left: 0, top: 20 }}>
                {index === statusTrail.length - 1 ? (
                  <CheckCircle 
                    sx={{ 
                      fontSize: 16, 
                      color: 'primary.main',
                      bgcolor: 'background.paper',
                      borderRadius: '50%'
                    }} 
                  />
                ) : (
                  <RadioButtonUnchecked 
                    sx={{ 
                      fontSize: 16, 
                      color: 'grey.400',
                      bgcolor: 'background.paper',
                      borderRadius: '50%'
                    }} 
                  />
                )}
              </Box>
              <Box sx={{ flex: 1 }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                  <Chip 
                    label={`Phase ${tick.phase}`}
                    size="small"
                    color="primary"
                    variant="outlined"
                    sx={{ height: 20, fontSize: '0.65rem' }}
                  />
                  <Typography variant="caption" color="text.secondary">
                    {tick.step}
                  </Typography>
                </Box>
                <Typography variant="body2" sx={{ mb: tick.summary ? 0.25 : 0.5, fontWeight: index === statusTrail.length - 1 ? 500 : 400 }}>
                  {tick.message}
                </Typography>
                {tick.summary && (
                  <Stack spacing={0.25} sx={{ mb: 0.5 }}>
                    {tick.summary.entities_count !== undefined && (
                      <Typography variant="caption" color="text.secondary">
                        Entities: {tick.summary.entities_count}
                        {Array.isArray(tick.summary.entities_sample) && tick.summary.entities_sample.length > 0 && ` (${tick.summary.entities_sample.join(', ')})`}
                      </Typography>
                    )}
                    {tick.summary.relations_count !== undefined && (
                      <Typography variant="caption" color="text.secondary">
                        Relations: {tick.summary.relations_count}
                        {Array.isArray(tick.summary.relations_sample) && tick.summary.relations_sample.length > 0 && ` (${tick.summary.relations_sample.map((r: any) => (r?.entities ? r.entities.join(' - ') : '')).join('; ')})`}
                      </Typography>
                    )}
                    {tick.summary.attributes_sample && tick.summary.attributes_sample.entity && (
                      <Typography variant="caption" color="text.secondary">
                        Attributes [{tick.summary.attributes_sample.entity}]: {Array.isArray(tick.summary.attributes_sample.attributes) ? tick.summary.attributes_sample.attributes.join(', ') : ''}
                      </Typography>
                    )}
                  </Stack>
                )}
                <Typography variant="caption" color="text.secondary" sx={{ fontSize: '0.7rem' }}>
                  {formatTimestamp(tick.ts)}
                </Typography>
              </Box>
            </Box>
            {index < statusTrail.length - 1 && <Divider sx={{ mx: 2 }} />}
          </Box>
        ))}
      </Box>
    </Paper>
  );
};

export default StatusTrail;

