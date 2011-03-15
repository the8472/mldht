/*
 *    This file is part of mlDHT. 
 * 
 *    mlDHT is free software: you can redistribute it and/or modify 
 *    it under the terms of the GNU General Public License as published by 
 *    the Free Software Foundation, either version 2 of the License, or 
 *    (at your option) any later version. 
 * 
 *    mlDHT is distributed in the hope that it will be useful, 
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of 
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 *    GNU General Public License for more details. 
 * 
 *    You should have received a copy of the GNU General Public License 
 *    along with mlDHT.  If not, see <http://www.gnu.org/licenses/>. 
 */
package lbms.plugins.mldht.kad.messages;

import java.io.IOException;
import java.util.AbstractSet;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import javax.naming.OperationNotSupportedException;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHTConstants;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.DHT.DHTtype;

/**
 * @author Damokles
 *
 */
public class UnknownTypeResponse extends FindNodeResponse {
	public UnknownTypeResponse (byte[] mtid, byte[] nodes, byte[] nodes6) {
		super(mtid, nodes, nodes6);
		this.nodes = nodes;
		this.nodes6 = nodes6;
		method = Method.UNKNOWN;
	}

	@Override
	public void apply (DHT dh_table) {
		throw new UnsupportedOperationException("incoming responses should never be 'unknown'");
	}
}
