import React from 'react';
import { Box, Typography, Paper, Collapse, IconButton, Chip } from '@mui/material';
import { ExpandMore, ExpandLess, Info } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import StatusTrail from './StatusTrail';

const StatusTicker: React.FC = () => {
  const { 
    latestStatusMessage, 
    statusTrail, 
    statusTrailExpanded, 
    setStatusTrailExpanded,
    processing 
  } = useAppStore();
  
  return (
    <Box sx={{ p: 4 }}>
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Info color="primary" />
          <Typography variant="h6" sx={{ fontWeight: 600 }}>
            Processing Status
          </Typography>
        </Box>
        {statusTrail.length > 0 && (
          <Chip 
            label={`${statusTrail.length} updates`}
            size="small"
            color="primary"
            variant="outlined"
          />
        )}
      </Box>
      
      <Paper
        elevation={0}
        onClick={() => setStatusTrailExpanded(!statusTrailExpanded)}
        sx={{
          p: 3,
          cursor: 'pointer',
          borderRadius: 2,
          bgcolor: processing ? 'action.hover' : 'grey.50',
          border: '1px solid',
          borderColor: processing ? 'primary.light' : 'divider',
          transition: 'all 0.2s ease',
          '&:hover': { 
            bgcolor: processing ? 'action.selected' : 'grey.100',
            borderColor: processing ? 'primary.main' : 'grey.300',
          }
        }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <Typography 
            variant="body1" 
            sx={{ 
              fontWeight: processing ? 600 : 400,
              color: processing ? 'primary.main' : 'text.secondary',
              flex: 1
            }}
          >
            {latestStatusMessage || "Waiting for processing to start..."}
          </Typography>
          {statusTrail.length > 0 && (
            <IconButton 
              size="small" 
              sx={{ ml: 2 }}
              onClick={(e) => {
                e.stopPropagation();
                setStatusTrailExpanded(!statusTrailExpanded);
              }}
            >
              {statusTrailExpanded ? <ExpandLess /> : <ExpandMore />}
            </IconButton>
          )}
        </Box>
      </Paper>
      
      <Collapse in={statusTrailExpanded} timeout="auto">
        <Box sx={{ mt: 2 }}>
          <StatusTrail />
        </Box>
      </Collapse>
    </Box>
  );
};

export default StatusTicker;

