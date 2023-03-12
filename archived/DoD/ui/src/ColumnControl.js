import React, { Component } from 'react';

function ColumnButtonPanel(props) {

	const nextCol = props.next_col;
	const prvCol = props.prv_col;

	return (
		<div>
            <button type="button" className="button-prv" data-toggle="tooltip" data-placement="left" data-html="true" title="back" onClick={prvCol}>
			    <span className="fas fa-backward" aria-hidden="true"></span>
			</button>
			<button type="button" className="button-next" data-toggle="tooltip" data-placement="left" data-html="true" title="next" onClick={nextCol}>
			    <span className="fas fa-forward"></span>
			</button>

		</div>
	)
}

function ColumnControl(props) {
		return (
			<div className="ColumnControl" id='column-control'>

		        <ColumnButtonPanel next_col={props.next_col}
							 prv_col={props.prv_col}
			    />
			</div>
		)

}

export default ColumnControl;