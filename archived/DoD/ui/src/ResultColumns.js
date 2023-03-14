import React from 'react';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Button from '@material-ui/core/Button';
import Card from '@material-ui/core/Card';
import CardActions from '@material-ui/core/CardActions';
import CardContent from '@material-ui/core/CardContent';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from "@material-ui/core/DialogTitle/DialogTitle";
import List from "@material-ui/core/List/List";
import ListItem from "@material-ui/core/ListItem/ListItem";
import Dialog from "@material-ui/core/Dialog/Dialog";

function Cluster (props) {

    const [open, setOpen] = React.useState(false);

    const handleClickOpen = () => {
        setOpen(true);
    }

    const handleClose = () => {
        setOpen(false);
    };
        return (
            <div align="center">
                <Card>
                    <CardContent>
                <h6>Cluster#{props.index} </h6>
                <h6>Sample Hit Score: {props.sample_score}</h6>
                <Table>
                    <TableHead>
                    <TableCell>Table</TableCell>
                    <TableCell>Column</TableCell>
                    <TableCell>attr_score</TableCell>
                    <TableCell>check</TableCell>
                    </TableHead>
                    <TableBody>
                    {
                        props.data.map((row, index)=>{
                            return (
                                <TableRow key={index}>
                                    {
                                        row.map((cell,index)=>{
                                        return <TableCell key={index}>{cell}</TableCell>
                                    })
                  }
                  <TableCell><input type="checkbox"/></TableCell>
                 </TableRow>
)
                        })
                    }
                    </TableBody>
                </Table>
                    </CardContent>
                    <CardActions>
                       <Button size="small" onClick={handleClickOpen}>Learn More</Button>
                       <Dialog onClose={handleClose} aria-labelledby="simple-dialog-title" open={open}>
                            <DialogTitle id="simple-dialog-title">Column Detail</DialogTitle>
                           <DialogContent >
                            <List>
                                {props.headValues.map((cell, index) => (
                            <ListItem key={index}>
                                <div dangerouslySetInnerHTML = {{ __html:cell }}></div>
                            </ListItem>
                        ))}
                            </List>
                           </DialogContent>
                           <DialogActions>
                                <Button onClick={handleClose} color="primary">
                                Close
                                </Button>
                           </DialogActions>
                         </Dialog>
                    </CardActions>
                </Card>
                <br/>
            </div>
        )

}

function ResultColumns(props) {

    return (
        <div className="ResultColumns ">
            <div id="clusterView">
                <h4>{props.name}</h4>
                {props.clusters.map((cluster, index) => <Cluster index={index+1} sample_score={cluster.sample_score} data={cluster.data} headValues={cluster.head_values} />)}
                </div>
        </div>
    )

}

export default ResultColumns;