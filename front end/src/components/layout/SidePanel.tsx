import React, { useState } from 'react';
import { Box, IconButton, Collapse, Divider } from '@mui/material';
import { ChevronLeft, ChevronRight } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import SuggestionsPanel from '../nl-input/SuggestionsPanel';

const SidePanel: React.FC = () => {
  const { processing } = useAppStore();
  const [expanded, setExpanded] = useState(!processing); // Collapsed by default when processing
  
  // Auto-collapse when processing starts
  React.useEffect(() => {
    if (processing) {
      setExpanded(false);
    }
  }, [processing]);
  
  return (
    <Box 
      sx={{ 
        width: expanded ? 380 : 0,
        borderLeft: '1px solid',
        borderColor: 'divider',
        bgcolor: 'background.paper',
        display: 'flex',
        flexDirection: 'column',
        overflow: 'hidden',
        transition: 'width 0.3s ease',
        position: 'relative'
      }}
    >
      <IconButton
        onClick={() => setExpanded(!expanded)}
        sx={{
          position: 'absolute',
          left: expanded ? -20 : -40,
          top: 20,
          zIndex: 10,
          bgcolor: 'background.paper',
          border: '1px solid',
          borderColor: 'divider',
          boxShadow: 1,
          '&:hover': {
            bgcolor: 'action.hover',
          },
          transition: 'left 0.3s ease',
        }}
      >
        {expanded ? <ChevronLeft /> : <ChevronRight />}
      </IconButton>
      
      <Collapse in={expanded} orientation="horizontal">
        <Box sx={{ width: 380, height: '100%', display: 'flex', flexDirection: 'column' }}>
          <SuggestionsPanel />
        </Box>
      </Collapse>
    </Box>
  );
};

export default SidePanel;

