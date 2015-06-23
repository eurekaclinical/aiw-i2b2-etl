package edu.emory.cci.aiw.neo4jetl;

/*
 * #%L
 * AIW Neo4j ETL
 * %%
 * Copyright (C) 2015 Emory University
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-3.0.html>.
 * #L%
 */
import edu.emory.cci.aiw.neo4jetl.config.Configuration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.protempa.ParameterDefinition;
import org.protempa.PropositionDefinition;

import org.protempa.proposition.AbstractParameter;
import org.protempa.proposition.Constant;
import org.protempa.proposition.Context;
import org.protempa.proposition.Event;
import org.protempa.proposition.Parameter;
import org.protempa.proposition.PrimitiveParameter;
import org.protempa.proposition.Proposition;
import org.protempa.proposition.TemporalProposition;
import org.protempa.proposition.interval.Interval;
import org.protempa.proposition.value.Value;
import org.protempa.proposition.value.ValueType;
import org.protempa.proposition.visitor.PropositionVisitor;

/**
 * @author hrathod
 */
class MapPropositionVisitor implements PropositionVisitor {

	private final Map<String, Object> map = new HashMap<>();
	private final Configuration configuration;
	private final Map<String, PropositionDefinition> cache;

	MapPropositionVisitor(Configuration configuration, Map<String, PropositionDefinition> cache) {
		this.configuration = configuration;
		this.cache = cache;
	}
	
	@Override
	public void visit(Map<String, List<Proposition>> finderResult) {
		throw new UnsupportedOperationException("Not supported yet");
	}

	@Override
	public void visit(Collection<? extends Proposition> propositions) {
		throw new UnsupportedOperationException("Not supported yet");
	}

	@Override
	public void visit(PrimitiveParameter primitiveParameter) {
		handleTemporal(primitiveParameter);
		this.map.put("position", primitiveParameter.getPosition());
		this.map.put("position_tval", primitiveParameter.getPositionFormattedShort());
		handleValue(primitiveParameter);
		handleCommon(primitiveParameter);
	}

	@Override
	public void visit(Event event) {
		handleTemporal(event);
		handleCommon(event);
	}

	@Override
	public void visit(AbstractParameter abstractParameter) {
		handleTemporal(abstractParameter);
		handleValue(abstractParameter);
		handleCommon(abstractParameter);
	}

	@Override
	public void visit(Constant constant) {
		this.handleCommon(constant);
	}

	@Override
	public void visit(Context context) {
		this.handleCommon(context);
	}

	public Map<String, Object> getMap() {
		return map;
	}

	private void handleCommon(Proposition inProposition) {
		for (String s : inProposition.getPropertyNames()) {
			Value v = inProposition.getProperty(s);
			if (v != null) {
				RawValueVisitor visitor = new RawValueVisitor();
				v.accept(visitor);
				this.map.put(s, visitor.getValue());
			} else {
				this.map.put(s, this.configuration.getNullValue());
			}
		}
	}

	private void handleTemporal(TemporalProposition tempProp) {
		Interval interval = tempProp.getInterval();
		this.map.put("length", interval.getMinLength());
		this.map.put("lengthUnit", tempProp.getLengthFormattedShort());
		this.map.put("start", interval.getMinStart());
		this.map.put("start_tval", tempProp.getStartFormattedShort());
		this.map.put("finish", interval.getMinFinish());
		this.map.put("finish_tval", tempProp.getFinishFormattedShort());
	}

	private void handleValue(Parameter param) {
		Value value = param.getValue();
		if (value != null) {
			RawValueVisitor visitor = new RawValueVisitor();
			value.accept(visitor);
			this.map.put("value", visitor.getValue());
			this.map.put("value_tval", value.getFormatted());
			this.map.put("valueType", value.getType().name());
		} else {
			this.map.put("value", this.configuration.getNullValue());
			this.map.put("value_tval", this.configuration.getNullValue());
			this.map.put("valueType", ((ParameterDefinition) this.cache.get(param.getId())).getValueType().name());
		}
	}
}
